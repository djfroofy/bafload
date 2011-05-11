# Copyright (c) 2011 Drew Smathers
# See LICENSE for details
import sys
import os
import optparse
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

    def __init__(self, total):
        self.completed = 0
        self.total = total

    def count(self, fd, piece):
        self.completed += 1
        sys.stderr.write('%-40s \r' % ('transferred chunks %d/%d' % (self.completed, self.total)))


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


def upload_multipart(bucket, path, threads=0):
    key = path
    sys.stderr.write('bucket=%s, key=%s\n' % (bucket, key))
    conn = boto.connect_s3()
    bucket = conn.lookup(bucket)
    mupload = bucket.initiate_multipart_upload(key)
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

def main():
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bucket', dest='bucket', help='The bucket name')
    parser.add_option('-t', '--threads', dest='threads', type='int', default=0,
                      help='Number of threads to use')
    opts, args = parser.parse_args()
    path = args[0]
    upload_multipart(opts.bucket, path, opts.threads)

if __name__ == '__main__':
    main()


