import os

from zope.interface import implements

from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.task import coiterate

from txaws.credentials import AWSCredentials
from txaws.service import AWSServiceRegion

from bafload.interfaces import (ITransmissionCounter, IPartHandler,
    IPartsGenerator, IMultipartUploadsManager)
from bafload.common import BaseCounter, ProgressLoggerMixin


class PartsTransferredCounter(BaseCounter):
    receiving = False


class FileIOPartsGenerator(ProgressLoggerMixin):
    implements(IPartsGenerator)

    def generate_parts(self, fd):
        # FIXME
        return iter([])


class SingleProcessPartUploader(ProgressLoggerMixin):
    implements(IPartHandler)

    bucket = None
    object_name = None
    upload_id = None
    client = None

    # TODO - change seq_no to part_number for consistency
    # here and everywhere else
    def handle_part(self, bytes, seq_no):
        d = self.client.upload_part(self.bucket, self.object_name,
            self.upload_id, seq_no, bytes)
        d.addCallback(self._handle_headers, seq_no)
        return d

    def _handle_headers(self, headers, seq_no):
        return (headers['ETag'], seq_no)


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
        self.init_response = response
        return coiterate(self._generate_parts(
            self.parts_generator.generate_parts(self.fd)))

    def _generate_parts(self, gen):
        work = []
        self.parts_list = []
        for (bytes, seq_no) in gen:
            d = self.part_handler.handle_part(bytes, seq_no)
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
        return '%s upload_id=%s' % (cname, upid)


class MultipartUploadsManager(ProgressLoggerMixin):
    """
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
        # TODO - probably need some pluggable strategy for getting the parts
        # count (if desired) or not (optimization) - maybe parts_count()
        # method on IPartsGenerator.
        # Or maybe this whole counter idea is just plain wrong.
        counter = self.counter_factory('?')
        if parts_generator is None:
            parts_generator = FileIOPartsGenerator()
        if part_handler is None:
            part_handler = SingleProcessPartUploader()
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

