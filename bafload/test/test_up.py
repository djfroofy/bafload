from zope.interface.verify import verifyClass, verifyObject

from twisted.trial.unittest import TestCase

from bafload.interfaces import (IPartsGenerator, ITransmissionCounter,
    IPartHandler, IMultipartUploader)
from bafload.up import (FileIOPartsGenerator, PartsTransferredCounter,
    SingleProcessPartUploader, MultipartUploader)

class InterfacesTestCase(TestCase):

    def test_file_io_parts_generator_ifaces(self):
        verifyClass(IPartsGenerator, FileIOPartsGenerator)
        parts_gen = FileIOPartsGenerator()
        verifyObject(IPartsGenerator, parts_gen)

    def test_parts_transmission_counter_ifaces(self):
        verifyClass(ITransmissionCounter, PartsTransferredCounter)
        counter = PartsTransferredCounter(365)
        verifyObject(ITransmissionCounter, counter)

    def test_single_process_part_uploader_ifaces(self):
        verifyClass(IPartHandler, SingleProcessPartUploader)
        uploader = SingleProcessPartUploader()
        verifyObject(IPartHandler, uploader)

    def test_multipart_upload_ifaces(self):
        verifyClass(IMultipartUploader, MultipartUploader)
        mp_uploader = MultipartUploader()
        verifyObject(IMultipartUploader, mp_uploader)

