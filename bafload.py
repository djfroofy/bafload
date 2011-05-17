# Copyright (c) 2011 Drew Smathers
# See LICENSE for details
import sys
import os
import optparse
import time
from Queue import Queue

import boto
try:
    from twisted.python import log
    from twisted.internet import reactor
    from twisted.internet.defer import gatherResults
    from twisted.internet.threads import deferToThread
    from twisted.internet.task import coiterate
    thread_pool = 1
except ImportError:
    thread_pool = 0
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

CHUNK_SIZE = 5 * 1024 * 1024
DONE = object()

class MultipartUploadTask:

    def __init__(self, bucket, multipart_upload_id, queue, callback):
        self.bucket = bucket
        self.multipart_upload_id = multipart_upload_id
        self.queue = queue
        self.callback = callback

    def _write_init(self):
        conn = boto.connect_s3()
        bucket = conn.lookup(self.bucket)
        for mu in bucket.get_all_multipart_uploads():
            self.mupload = mu
            if mu.id == self.multipart_upload_id:
                break
        else:
            raise ValueError('no multipart upload exists for id: %s' %
                             self.multipart_upload_id)

    def write(self):
        self._write_init()
        while 1:
            next = self.queue.get()
            if next == DONE:
                return
            fd, piece = next
            self._write(fd, piece)

    def _write(self, fd, piece):
        while 1:
            try:
                self.mupload.upload_part_from_file(fd, piece)
            except Exception, e:
                reactor.callFromThread(sys.stderr.write,
                    'Failure writing piece %d. Trying again.\n' % piece)
                time.sleep(0.1)
            else:
                break
        reactor.callFromThread(self.callback, fd, piece)


class CompletionCounter:

    def __init__(self, total, sending=True):
        self.completed = 0
        self.total = total
        self.sending = sending

    def count(self, fd, piece):
        self.completed += 1
        verb = ('received', 'transferred')[self.sending]
        sys.stderr.write('%-40s \r' % ('%s chunks %d/%d' % (verb, self.completed, self.total)))


def generate_chunk_files(path, feedback=False):
    size = os.stat(path).st_size
    chunks = size  / CHUNK_SIZE
    rem = size % CHUNK_SIZE
    bytes = 0
    with open(path, 'rb') as fd:
        for ct in range(chunks + 1):
            amount = CHUNK_SIZE
            if ct == chunks:
                amount = rem
            chunk = fd.read(amount)
            if feedback:
                sys.stderr.write('%-40s \r' % ('uploading chunk %d/%d' % (ct + 1, chunks + 1)))
            yield StringIO(chunk)


def upload_multipart(bucket, path, threads=0, public=False):
    key = path
    sys.stderr.write('bucket=%s, key=%s\n' % (bucket, key))
    conn = boto.connect_s3()
    bucket = conn.lookup(bucket)
    headers = None
    if public:
        headers = { 'x-amz-acl' : 'public-read' }
    mupload = bucket.initiate_multipart_upload(key, headers=headers)
    if threads:
        if not thread_pool:
            raise RuntimeError("Twisted is required for threads - ain't that ironic?")
        reactor.callWhenRunning(upload_multipart_parallel, bucket, path, mupload, threads)
        reactor.run()
    else:
        for (piece, fd) in enumerate(generate_chunk_files(path, True), 1):
            mupload.upload_part_from_file(fd, piece)
        mupload.complete_upload()
        sys.stderr.write('\ndone\n')


def upload_multipart_parallel(bucket, path, mupload, threads):
    sys.stderr.write('thread count: %d\n' %  threads)
    reactor.suggestThreadPoolSize(threads)
    chunks = (os.stat(path).st_size / CHUNK_SIZE) + 1

    counter = CompletionCounter(chunks)
    q = Queue(threads * 4)
    deferreds = []
    for i in range(threads):
        task = MultipartUploadTask(bucket, mupload.id, q, counter.count)
        deferreds.append(deferToThread(task.write))

    def end(ignore):
        sys.stderr.write('\ndone\n')
        mupload.complete_upload()
        reactor.stop()
    gatherResults(deferreds).addErrback(log.err).addCallback(end)

    def chunker():
        for (piece, fd) in enumerate(generate_chunk_files(path), 1):
            q.put((fd, piece))
            yield
        for i in range(threads):
            q.put(DONE)
            yield
    chunker = chunker()
    return coiterate(chunker).addCallback(
        lambda ign: sys.stderr.write('\nfinished generating chunks for threads\n'))


class DownloadChunkMultiplexer(object):

    def __init__(self, path):
        self.file = open(path, 'w+b')

    def update(self, piece, chunk):
        start = piece * CHUNK_SIZE
        self.file.seek(start)
        self.file.write(chunk)

    def close(self):
        self.file.close()


class DownloadTask(object):

    def __init__(self, bucket, key, queue, download_manager, callback):
        self.bucket = bucket
        self.key = key
        self.queue = queue
        self.download_manager = download_manager
        self.callback = callback
        self.buf = StringIO()

    def _get_key(self, key, bucket=None):
        if not bucket:
            conn = boto.connect_s3()
            bucket = conn.lookup(self.bucket)
        return bucket, bucket.get_key(key)

    def get(self):
        bucket, key = self._get_key(self.key)
        while 1:
            message = self.queue.get()
            if message == DONE:
                return
            piece, start, end = message
            bucket, key = self._get(piece, start, end, bucket, key)

    def _get(self, piece, start, end, bucket, key):
        while 1:
            try:
                self.buf.seek(0)
                self.buf.truncate()
                key.get_contents_to_file(self.buf, headers={'Range':'bytes=%d-%d' % (start, end)})
                reactor.callFromThread(self.download_manager.update, piece, self.buf.getvalue())
            except Exception, e:
                reactor.callFromThread(sys.stderr.write,
                    'Failure getting piece %d. (%s) Trying again.\n' % (piece, e))
                bucket, key = self._get_key(self.key)
                time.sleep(0.1)
            else:
                break
        bucket, key = self._get_key(self.key, bucket)
        reactor.callFromThread(self.callback, None, piece)
        return bucket, key

def download_ranges(bucket, s3path, localpath, threads):
    sys.stderr.write('thread count: %d\n' %  threads)
    reactor.suggestThreadPoolSize(threads)
    reactor.callWhenRunning(_download_ranges, bucket, s3path, localpath, threads)
    reactor.run()

def _download_ranges(bucket, s3path, localpath, threads):
    conn = boto.connect_s3()
    bucket2 = conn.lookup(bucket)
    key = bucket2.get_key(s3path)
    size = key.size
    chunks = (size / CHUNK_SIZE) + 1

    if not localpath:
        localpath = os.path.basename(s3path)

    counter = CompletionCounter(chunks, sending=False)
    q = Queue(threads * 4)
    download_manager = DownloadChunkMultiplexer(localpath)
    deferreds = []
    for i in range(threads):
        task = DownloadTask(bucket, s3path, q, download_manager, counter.count)
        deferreds.append(deferToThread(task.get))

    def end(ignore):
        sys.stderr.write('\ndone\n')
        download_manager.close()
        reactor.stop()
    gatherResults(deferreds).addErrback(log.err).addCallback(end)

    def ranger():
        for index in range(chunks):
            start = CHUNK_SIZE * index
            if start == size:
                break
            end = min(start + CHUNK_SIZE - 1, size)
            q.put((index, start, end))
            yield
        for i in range(threads):
            q.put(DONE)
            yield
    ranger = ranger()
    return coiterate(ranger).addCallback(
        lambda ign: sys.stderr.write('\nfinished generating ranges for threads\n'))


def main():
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bucket', dest='bucket', help='The bucket name')
    parser.add_option('-t', '--threads', dest='threads', type='int', default=0,
                      help='Number of threads to use')
    parser.add_option('-p', '--public', dest='public', action='store_true',
                      help='Use this flag to set acl to public-read')
    parser.add_option('-d', '--download', dest='download', action='store_true',
                      help='Download a resource grabbing chunks in parallel')
    parser.add_option('-o', '--output-file', dest='outputfile',
                      help='For download, the output file to save to. '
                           'If not given the basename of the key will be used.')
    opts, args = parser.parse_args()
    path = args[0]
    if opts.download:
        sys.stderr.write('downloading %s\n' % path)
        download_ranges(opts.bucket, path, opts.outputfile, opts.threads or 10)
    else:
        upload_multipart(opts.bucket, path, opts.threads, opts.public)

if __name__ == '__main__':
    main()


