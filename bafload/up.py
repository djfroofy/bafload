# Copryright 2012 Drew Smathers, See LICENSE
import os
import types

from zope.interface import implements

from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.task import coiterate

from txaws.credentials import AWSCredentials
from txaws.service import AWSServiceRegion

from bafload.interfaces import (ITransmissionCounter, IPartHandler,
    IPartsGenerator, IMultipartUploadsManager)
from bafload.common import BaseCounter, ProgressLoggerMixin


DEFAULT_PART_SIZE = 0x500000


class PartsTransferredCounter(BaseCounter):
    receiving = False


class FileIOPartsGenerator(ProgressLoggerMixin):
    implements(IPartsGenerator)

    part_size = DEFAULT_PART_SIZE

    def generate_parts(self, fd):
        seek = fd.seek
        read = fd.read
        size = self.part_size
        seek(0)
        part_number = 1
        part = read(size)
        while part:
            # TODO - the part generated should optimally be an
            # C{IBodyProducer} rather than reading 5MB of data
            # for each part into memory
            yield (part, part_number)
            part_number += 1
            part = read(size)

    def count_parts(self, fd):
        # XXX - maybe some stub interfaces and adaptors instead
        # of this messy type checking
        if isinstance(fd, types.FileType):
            size = os.fstat(fd.fileno()).st_size
        elif hasattr(fd, 'len'):
            size = fd.len
        else:
            return '?'
        count = size / self.part_size
        if size % self.part_size:
            return count + 1
        return count


class SingleProcessPartUploader(ProgressLoggerMixin):
    implements(IPartHandler)

    bucket = None
    object_name = None
    upload_id = None
    client = None

    def handle_part(self, part, part_number):
        d = self.client.upload_part(self.bucket, self.object_name,
            self.upload_id, part_number, part)
        d.addCallback(self._handle_headers, part_number)
        return d

    def _handle_headers(self, headers, part_number):
        return (part_number, headers['ETag'])


class MultipartTaskCompletion(object):

    def __init__(self, upload_id, task):
        self.upload_id = upload_id
        self.task = task


class MultipartUpload(ProgressLoggerMixin):

    init_response = None
    completion_response = None

    def __init__(self, client, fd, parts_generator, part_handler, counter,
                 finished, log=None):
        self.client = client
        self.fd = fd
        self.parts_generator = parts_generator
        self.part_handler = part_handler
        self.counter = counter
        self.finished = finished
        self.set_log(log)

    def upload(self, bucket, object_name, content_type, metadata):
        self.part_handler.bucket = bucket
        self.part_handler.object_name = object_name
        d = self.client.init_multipart_upload(bucket, object_name,
            content_type, metadata)
        return d.addCallback(self._initialized)

    def _initialized(self, response):
        self.part_handler.upload_id = response.upload_id
        self.init_response = response
        return coiterate(self._generate_parts(
            self.parts_generator.generate_parts(self.fd)))

    def _generate_parts(self, gen):
        def count(result):
            self.counter.increment_count()
            return result
        work = []
        for (part, part_number) in gen:
            d = self.part_handler.handle_part(part, part_number)
            d.addCallback(count)
            work.append(d)
            yield
        d = DeferredList(work)
        d.addCallback(self._parts_uploaded)

    def _parts_uploaded(self, result):
        # TODO - handle errors here. possible create a new generator
        # to pass back to generate parts and try again X times for
        # failed parts?
        parts_list = [ r[1] for r in  result ]
        bucket = self.part_handler.bucket
        object_name = self.part_handler.object_name
        upload_id = self.init_response.upload_id
        d = self.client.complete_multipart_upload(bucket, object_name,
            upload_id, parts_list)
        d.addCallback(self._completed)

    def _completed(self, completion_response):
        self.completion_response = completion_response
        d = self.finished
        self.finished = None
        d.callback(self)

    def __str__(self):
        cname = self.__class__.__name__
        upid = None
        if self.init_response is not None:
            upid = self.init_response.upload_id
        s = '%s upload_id=%s' % (cname, upid)
        if self.part_handler:
            s += ', bucket=%s, object_name=%s' % (
                self.part_handler.bucket, self.part_handler.object_name)
        return s

class MultipartUploadsManager(ProgressLoggerMixin):
    """
    The L{MultipartUploadsManager} is the primary interface for optionally
    queuing and performing multipart uploads for files/data.

    @param creds: L{txaws.credentials.AWSCredentials} object or if None this
        will be contruscted based on environment variables AWS_ACCESS_KEY_ID
        and AWS_SECRET_ACCESS_KEY.
    @param counter_factory: A callable that takes no args and returns an
        L{ITransmissionCounter} provider. If None, the default class
        L{PartsTransferredCounter} will be used.
    @param log: An L{ILog} provider. default: L{twisted.python.log}
    """
    implements(IMultipartUploadsManager)

    def __init__(self, creds=None, counter_factory=None, log=None, region=None):
        if counter_factory is None:
            counter_factory = PartsTransferredCounter
        if region is None:
            region = AWSServiceRegion(creds=creds)
        self.region = region
        self.counter_factory = counter_factory
        self.set_log(log)
        self.uploads = set()

    def upload(self, fd, bucket, object_name, content_type=None,
               metadata={}, parts_generator=None, part_handler=None):
        self.log.msg('Beginning upload to bucket=%s,key=%s' % (
                     bucket, object_name))
        client = self.region.get_s3_client()
        if parts_generator is None:
            parts_generator = FileIOPartsGenerator()
        if part_handler is None:
            part_handler = SingleProcessPartUploader()
        # TODO - probably need some pluggable strategy for getting the parts
        # count (if desired) or not (optimization) - maybe parts_count()
        # method on IPartsGenerator.
        # Or maybe this whole counter idea is just plain wrong.
        counter = self.counter_factory(parts_generator.count_parts(fd))
        counter.context = '[object_name=%s] ' % object_name
        part_handler.client = client
        d = Deferred()
        task = MultipartUpload(client, fd, parts_generator, part_handler,
                               counter, d, self.log)
        self.uploads.add(task)
        d.addCallbacks(self._completed_upload, self.log.err)\
            .addBoth(self._remove_upload, task)
        task.upload(bucket, object_name, content_type, metadata)
        return d

    def _completed_upload(self, task):
        self.log.msg('Completed upload for task: %s' % task)
        return task

    def _remove_upload(self, _ignore, task):
        self.uploads.remove(task)
        return task

