from StringIO import StringIO

from zope.interface.verify import verifyClass, verifyObject

from twisted.trial.unittest import TestCase

from bafload.interfaces import (IPartsGenerator, ITransmissionCounter,
    IPartHandler, IMultipartUploadsManager)
from bafload.up import (FileIOPartsGenerator, PartsTransferredCounter,
    SingleProcessPartUploader, MultipartUploadsManager, MultipartUpload)
from bafload.test.util import FakeLog
from bafload import up as up_module

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
        verifyClass(IMultipartUploadsManager, MultipartUploadsManager)
        mp_uploader = MultipartUploadsManager()
        verifyObject(IMultipartUploadsManager, mp_uploader)


class TestMultipartUpload(MultipartUpload):

    def upload(self, bucket, object_name, content_type, metadata):
        self.bucket = bucket
        self.object_name = object_name
        self.metadata = metadata
        self.finished.callback(self)


class MultipastUploadTestCase(TestCase):

    def test_str(self):
        upload = MultipartUpload(None, None, None, None, None)
        upload.upload_id = "1234567890"
        self.assertEqual(str(upload), "MultipartUpload upload_id=1234567890")


class MultipartUploadsManagerTestCase(TestCase):

    def setUp(self):
        super(MultipartUploadsManagerTestCase, self).setUp()
        self.log = FakeLog()

    def test_upload_creation(self):
        self.patch(up_module, 'MultipartUpload', TestMultipartUpload)
        def check(task):
            self.assertIsInstance(task, MultipartUpload)
            self.assertEqual(task.bucket, "mybucket")
            self.assertEqual(task.object_name, "mykey")
            self.assertIdentical(task.fd, fd)
            self.assertEqual(task.metadata, {})
            self.assertIsInstance(task.counter, PartsTransferredCounter)
            self.assert_(self.log.buffer)
            for entry in self.log.buffer:
                self.assertEqual(entry[0], 'msg')
        manager = MultipartUploadsManager(log=self.log)
        fd = StringIO("some data")
        d = manager.upload(fd, 'mybucket', 'mykey')
        d.addCallback(check)
        return d
