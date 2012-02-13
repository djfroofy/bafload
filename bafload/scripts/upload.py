import sys
import os
from optparse import OptionParser
import mimetypes

from twisted.internet import reactor
from twisted.python import log
from twisted.internet.defer import gatherResults

from txaws.credentials import AWSCredentials
from txaws.service import AWSServiceRegion

from bafload.up import MultipartUploadsManager


parser = OptionParser()
parser.add_option('-a', '--acess-key', dest='access_key',
    help='AWS Acess Key ID')
parser.add_option('-s', '--secret-key', dest='secret_key',
    help='AWS Secret Access Key')
parser.add_option('-r', '--region', dest='region', default='US',
    help='EC2 Service Regison')
parser.add_option('-b', '--bucket', dest='bucket',
    help='Name of the bucket to upload to')
options, paths = parser.parse_args()


def error(message):
    sys.stderr.write(message + '\n')
    reactor.stop()


def complete(result):
    print 'successfully uploaded: %s' % ', '.join(paths)
    return result


def stop(ignore):
    reactor.stop()


def start():
    bucket = options.bucket
    if not bucket:
        return error("Must supply a bucket name!")
    creds = AWSCredentials(options.access_key, options.secret_key)
    region = AWSServiceRegion(creds=creds, region=options.region)
    uploader = MultipartUploadsManager(region=region)
    finished = []
    for path in paths:
        fd = open(path)
        object_name = os.path.basename(path)
        content_type = mimetypes.guess_type(path)[0]
        d = uploader.upload(fd, bucket, object_name, content_type=content_type)
        finished.append(d)
    gatherResults(finished).addCallback(complete, log.err).addBoth(stop)


log.startLogging(sys.stdout)
reactor.callWhenRunning(start)
reactor.run()
