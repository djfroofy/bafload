from zope.interface import implements

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

class MultipartUploader(ProgressLoggerMixin):
    implements(IMultipartUploader)


    def __init__(self, counter_factory=None, log_progress=True,
                 log=None):
        if counter_factory is None:
            counter_factory = PartsTransferredCounter
        self.counter_factory = counter_factory


    def upload(self, fd, bucket, object_name, content_type=None,
               metadata={}, parts_generator=None, part_handler=None):
        # FIXME
        pass

