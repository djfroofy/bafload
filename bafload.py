# Copyright (c) 2011 Drew Smathers
# See LICENSE for details
import sys
import os
import boto
import optparse
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

CHUNK_SIZE = 5 * 1024 * 1024

def generate_chunk_files(path):
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
            sys.stderr.write('%-40s \r' % ('uploading chunk %d/%d' % (ct + 1, chunks + 1)))
            yield StringIO(chunk)

def upload_multipart(bucket, path):
    key = path
    sys.stderr.write('bucket=%s, key=%s\n' % (bucket, key))
    conn = boto.connect_s3()
    bucket = conn.lookup(bucket)
    mupload = bucket.initiate_multipart_upload(key)
    for (piece, fd) in enumerate(generate_chunk_files(path), 1):
        mupload.upload_part_from_file(fd, piece)
    mupload.complete_upload()
    sys.stderr.write('\ndone\n')

def main():
    parser = optparse.OptionParser()
    parser.add_option('-b', '--bucket', dest='bucket', help='The bucket name')
    opts, args = parser.parse_args()
    path = args[0]
    upload_multipart(opts.bucket, path)

if __name__ == '__main__':
    main()


