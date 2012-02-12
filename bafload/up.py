from zope.interface import implements

from twisted.internet.defer import Deferred, gatherResults

from bafload.interfaces import (ITransmissionCounter, IPartHandler,
    IPartsGenerator, IMultipartUploader)
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

    def handle_part(self, bytes, seq_no):
        # FIXME
        pass


class MultipartTaskCompletion(object):

    def __init__(self, upload_id, task):
        self.upload_id = upload_id
        self.task = task


class MultipartUpload(ProgressLoggerMixin):

    upload_id = None

    def __init__(self, client, fd, parts_generator, part_handler, counter,
                 log=None):
        self.client = client
        self.fd = fd
        self.parts_generator = parts_generator
        self.part_handler = part_handler
        self.counter = counter
        self.finished = Deferred()
        self.set_log(log)

    def upload(self, bucket, object_name, content_type, metadata):
#        work = []
#        for (bytes, seq_no) in self.parts_generator.generate_parts():
#            d = self.part_handler.handle_part(bytes, seq_no)
#            work.append(d)
#            yield
#        gatherResults(work).addCallback(self._finalize)
        # FIXME
        self.finished.callback(self)

# FIXME
#    def _finalize(self):
#        d = self.finished
#        self.finished = None
#        d.callback(self)

    def __str__(self):
        cname = self.__class__.__name__
        return '%s upload_id=%s' % (cname, self.upload_id)


class MultipartUploader(ProgressLoggerMixin):
    implements(IMultipartUploader)

    def __init__(self, counter_factory=None, log=None):
        if counter_factory is None:
            counter_factory = PartsTransferredCounter
        self.counter_factory = counter_factory
        self.set_log(log)
        self.uploads = set()

    def upload(self, fd, bucket, object_name, content_type=None,
               metadata={}, parts_generator=None, part_handler=None):
        self.log.msg('Beginning upload to bucket=%s,key=%s' % (
                     bucket, object_name))
        # TODO - probably need some pluggable strategy for getting the parts
        # count (if desired) or not (optimization) - maybe parts_count()
        # method on IPartsGenerator
        counter = self.counter_factory('?')
        # FIXME will need to get actual client ... etc. etc.
        client = None
        parts_gen = None
        part_hdlr = None
        task = MultipartUpload(client, fd, parts_gen, part_hdlr, counter,
                               log=self.log)
        self.uploads.add(task)
        d = task.finished
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

