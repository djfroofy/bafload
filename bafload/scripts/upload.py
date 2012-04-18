import sys
import os
from optparse import OptionParser
import mimetypes

from twisted.internet import reactor
from twisted.python import log
from twisted.internet.defer import gatherResults

from txaws.credentials import AWSCredentials
from txaws.service import AWSServiceRegion

from bafload.stats import ThroughputCounter
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


def show_stats(result, throughput_counter):
    table = throughput_counter.read()
    slot_dur = throughput_counter.stats.slot_duration_secs
    (index0, (t0, _)) = [(i, (t, count))
            for (i, (t, count)) in enumerate(table) if count][0]
    (index1, (tk, _)) = [(i, (t, count))
            for (i, (t, count)) in enumerate(reversed(table)) if count][0]
    index1 = throughput_counter.stats.size - index1
    elapsed = (tk - t0) + slot_dur
    counts = [slot[1] for slot in table[index0:index1 + 1]]
    tx = sum(counts)
    max_count = max(counts)
    max_mbps = max_count / float(slot_dur) / (2 ** 20)
    min_mbps = min(counts) / float(slot_dur) / (2 ** 20)
    avg_mbps = tx / float(elapsed) / (2 ** 20)
    print 'Average Transfer: %3.3fMBs' % avg_mbps
    print 'Max: %3.3fMBs' % max_mbps
    print 'Min: %3.3fMBs' % min_mbps
    print 'Total tx: %2.2fMB' % (tx / 2.0 ** 20)
    return result


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
    throughput_counter = ThroughputCounter()
    uploader = MultipartUploadsManager(region=region,
            throughput_counter=throughput_counter)
    finished = []
    for path in paths:
        fd = open(path)
        object_name = os.path.basename(path)
        content_type = mimetypes.guess_type(path)[0]
        d = uploader.upload(fd, bucket, object_name, content_type=content_type,
                            amz_headers={'acl': 'public-read'})
        finished.append(d)
    gatherResults(finished).addCallback(show_stats, throughput_counter
            ).addCallbacks(complete, log.err).addBoth(stop)


log.startLogging(sys.stdout)
reactor.callWhenRunning(start)
reactor.run()
