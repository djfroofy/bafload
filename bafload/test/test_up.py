from StringIO import StringIO
from hashlib import md5

from zope.interface.verify import verifyClass, verifyObject
from zope.interface import implements

from twisted.internet.defer import Deferred, succeed
from twisted.python import log
from twisted.trial.unittest import TestCase

from txaws.s3.client import S3Client
from txaws.s3.model import (MultipartInitiationResponse,
    MultipartCompletionResponse)
from txaws.service import AWSServiceRegion
from txaws.credentials import AWSCredentials

from bafload.interfaces import (IPartsGenerator, ITransmissionCounter,
    IPartHandler, IMultipartUploadsManager)
from bafload.up import (FileIOPartsGenerator, PartsTransferredCounter,
    SingleProcessPartUploader, MultipartUploadsManager, MultipartUpload)
from bafload.test.util import FakeLog, FakeS3Client
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


class DummyPartsGenerator(object):
    implements(IPartsGenerator)

    def __init__(self):
        self.generated = []

    def generate_parts(self, fd):
        for i in range(10):
            entity = (chr(ord('a') + i) * 10, i+1)
            self.generated.append(entity)
            yield entity

class DummyPartHandler(object):

    def __init__(self):
        self.handled = []

    def handle_part(self, bytes, seq_no):
        self.handled.append((bytes, seq_no))
        etag = md5(bytes).hexdigest()
        return succeed((etag, seq_no))


class MultipartUploadTestCase(TestCase):

    def setUp(self):
        super(MultipartUploadTestCase, self).setUp()
        self.log = FakeLog()

    def test_str(self):
        upload = MultipartUpload(None, None, None, None, None, Deferred())
        r = MultipartInitiationResponse('', '', '1234567890')
        upload.init_response = r
        self.assertEqual(str(upload), "MultipartUpload upload_id=1234567890")

    def test_upload(self):
        client = FakeS3Client()
        parts_generator = DummyPartsGenerator()
        part_handler = DummyPartHandler()
        d = Deferred()
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            None, d, self.log)
        def check(task):
            self.assertIdentical(task, upload)
            expected = [('aaaaaaaaaa', 1),
                        ('bbbbbbbbbb', 2),
                        ('cccccccccc', 3),
                        ('dddddddddd', 4),
                        ('eeeeeeeeee', 5),
                        ('ffffffffff', 6),
                        ('gggggggggg', 7),
                        ('hhhhhhhhhh', 8),
                        ('iiiiiiiiii', 9),
                        ('jjjjjjjjjj', 10)]
            self.assertEqual(parts_generator.generated, expected)
            self.assertEqual(part_handler.handled, expected)
            self.assertIsInstance(task.init_response,
                MultipartInitiationResponse)
            self.assertIsInstance(task.completion_response,
                MultipartCompletionResponse)
        d.addCallback(check)
        upload.upload('mybucket', 'mykey', '', {})
        return d.addErrback(log.err)

class MultipartUploadsManagerTestCase(TestCase):

    def setUp(self):
        super(MultipartUploadsManagerTestCase, self).setUp()
        self.log = FakeLog()
        self.patch(up_module, 'MultipartUpload', TestMultipartUpload)

    def test_region(self):
        region = AWSServiceRegion()
        manager = MultipartUploadsManager(region=region)
        self.assertIdentical(manager.region, region)

    def test_creds(self):
        creds = AWSCredentials()
        manager = MultipartUploadsManager(creds=creds)
        self.assertIdentical(manager.region.creds, creds)

    def test_upload_creation(self):
        def check(task):
            self.assertIsInstance(task, MultipartUpload)
            self.assertEqual(task.bucket, "mybucket")
            self.assertEqual(task.object_name, "mykey")
            self.assertIdentical(task.fd, fd)
            self.assertEqual(task.metadata, {})
            self.assertIsInstance(task.counter, PartsTransferredCounter)
            self.assertIsInstance(task.client, S3Client)
            self.assert_(self.log.buffer)
            verifyObject(IPartsGenerator, task.parts_generator)
            verifyObject(IPartHandler, task.part_handler)
            for entry in self.log.buffer:
                self.assertEqual(entry[0], 'msg')
        manager = MultipartUploadsManager(log=self.log)
        fd = StringIO("some data")
        d = manager.upload(fd, 'mybucket', 'mykey')
        d.addCallback(check)
        return d
