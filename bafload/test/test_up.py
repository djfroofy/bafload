from StringIO import StringIO
from hashlib import md5

from zope.interface.verify import verifyClass, verifyObject
from zope.interface import implements

from twisted.internet.defer import Deferred, succeed, fail
from twisted.python import log
from twisted.trial.unittest import TestCase

from txaws.s3.client import S3Client
from txaws.s3.model import (MultipartInitiationResponse,
    MultipartCompletionResponse)
from txaws.service import AWSServiceRegion
from txaws.credentials import AWSCredentials

from bafload.interfaces import (IPartsGenerator, ITransmissionCounter,
    IPartHandler, IMultipartUploadsManager)
from bafload.errors import UploadPartError
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

    def upload(self, bucket, object_name, content_type, metadata,
               amz_headers={}):
        self.bucket = bucket
        self.object_name = object_name
        self.metadata = metadata
        self.amz_headers = amz_headers
        self.finished.callback(self)


class FileIOPartsGeneratorTestCase(TestCase):

    def setUp(self):
        super(FileIOPartsGeneratorTestCase, self).setUp()
        self.generator = FileIOPartsGenerator()
        self.generator.part_size = 10

    def test_generate_parts(self):
        fd = StringIO("x" * 5 + "y" * 10 + "z" * 13)
        generated = [ entity for entity in self.generator.generate_parts(fd) ]
        self.assertEqual(generated, [('xxxxxyyyyy', 1),
                                     ('yyyyyzzzzz', 2),
                                     ('zzzzzzzz', 3)])

    def test_default_part_size(self):
        self.assertEqual(FileIOPartsGenerator.part_size, 5 * 1024 * 1024)

    def test_count_parts_for_stringio_type(self):
        fd = StringIO("x" * 53)
        count = self.generator.count_parts(fd)
        self.assertEqual(count, 6)
        fd = StringIO("x" * 30)
        count = self.generator.count_parts(fd)
        self.assertEqual(count, 3)

    def test_count_parts_for_file_type(self):
        path = self.mktemp()
        with open(path, "w") as fd:
            fd.write("x" * 53)
        fd = open(path)
        count = self.generator.count_parts(fd)
        self.assertEqual(count, 6)

    def test_count_parts_for_other_type(self):
        count = self.generator.count_parts([])
        self.assertEqual(count, "?")


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

    def handle_part(self, part, part_number):
        self.handled.append((part, part_number))
        etag = md5(part).hexdigest()
        return succeed((etag, part_number))


class ErroringPartHandler(object):

    def __init__(self, error_count=3):
        self.handled = []
        self.error_count = error_count
        self.errors = {}

    def handle_part(self, part, part_number):
        self.errors.setdefault(part_number, [])
        if len(self.errors[part_number]) < self.error_count:
            self.errors[part_number].append(1)
            return fail(ValueError('woops'))
        self.handled.append((part, part_number))
        etag = md5(part).hexdigest()
        return succeed((etag, part_number))


class ErroringFakeS3Client(FakeS3Client):

    def __init__(self, error_count=3):
        FakeS3Client.__init__(self)
        self.error_count = error_count

    def complete_multipart_upload(self, *p, **kw):
        if not self.error_count:
            return FakeS3Client.complete_multipart_upload(self, *p, **kw)
        self.error_count -= 1
        return fail(ValueError('woops'))


class SingleProcessPartUploaderTestCase(TestCase):

    def test_handle_part(self):
        client = FakeS3Client()
        handler = SingleProcessPartUploader()
        handler.bucket = "mybucket"
        handler.object_name = "mykey"
        handler.upload_id = "theupload"
        handler.client = client
        def check(result):
            self.assertEqual(result, (1, '"0123456789"'))
        d = handler.handle_part("aaaaaaaaaa", 1)
        d.addCallback(check)
        return d

class MultipartUploadTestCase(TestCase):

    def setUp(self):
        super(MultipartUploadTestCase, self).setUp()
        self.log = FakeLog()
        self.delayed_calls = []
        self.patch(up_module, 'reactor', self)

    def callLater(self, delay, f, *a, **kw):
        self.delayed_calls.append((delay, f, a, kw))
        f(*a, **kw)

    def test_str(self):
        upload = MultipartUpload(None, None, None, None, None, Deferred())
        r = MultipartInitiationResponse('', '', '1234567890')
        upload.init_response = r
        self.assertEqual(str(upload), "MultipartUpload upload_id=1234567890")

    def _assertPartsCompleted(self, parts_generator, part_handler, received,
                              task, client):
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
        expected = [
            ('e09c80c42fda55f9d992e59ca6b3307d', 1),
            ('82136b4240d6ce4ea7d03e51469a393b', 2),
            ('e9b3390206d8dfc5ffc9b09284c0bbde', 3),
            ('a9e49c7aefe022f0a8540361cce7575c', 4),
            ('c5ba867d9056b7cecf87f8ce88af90f8', 5),
            ('66952c6203ae23242590c0061675234d', 6),
            ('fdc68ea4cf2763996cf215451b291c63', 7),
            ('70270ca63a3de2d8905a9181a0245e58', 8),
            ('d98bcb28df1de541e95a6722c5e983ea', 9),
            ('c71a8da22bf4053760a604897627474c', 10),]
        self.assertEqual(received, expected)
        self.assertIsInstance(task.init_response,
            MultipartInitiationResponse)
        self.assertIsInstance(task.completion_response,
            MultipartCompletionResponse)
        self.assertEqual(client.calls, [
            ('init_multipart_upload', 'mybucket',
             'mykey', '', {}, {'acl': 'public-read'})])


    def test_upload(self):
        client = FakeS3Client()
        parts_generator = DummyPartsGenerator()
        part_handler = DummyPartHandler()
        counter = PartsTransferredCounter('?')
        d = Deferred()
        received = []
        amz_headers = {'acl': 'public-read'}
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            counter, d, self.log)
        def check(task):
            self.assertIdentical(task, upload)
            self._assertPartsCompleted(parts_generator, part_handler,
                                      received, task, client)
        d.addCallback(check)
        received = []
        upload.on_part_generated = received.append
        upload.upload('mybucket', 'mykey', '', {}, amz_headers)
        return d

    def test_upload_error_recovery(self):
        client = FakeS3Client()
        parts_generator = DummyPartsGenerator()
        part_handler = ErroringPartHandler()
        counter = PartsTransferredCounter('?')
        d = Deferred()
        received = []
        amz_headers = {'acl': 'public-read'}
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            counter, d, self.log)
        def check(task):
            self.flushLoggedErrors()
            self.assertEqual(len(self.delayed_calls), 3)
            self.assertIdentical(task, upload)
            self._assertPartsCompleted(parts_generator, part_handler,
                                      received, task, client)
        d.addCallback(check)
        received = []
        upload.on_part_generated = received.append
        upload.upload('mybucket', 'mykey', '', {}, amz_headers)
        return d

    def test_upload_error_timeout_finally(self):
        client = FakeS3Client()
        parts_generator = DummyPartsGenerator()
        part_handler = ErroringPartHandler(100)
        counter = PartsTransferredCounter('?')
        d = Deferred()
        received = []
        amz_headers = {'acl': 'public-read'}
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            counter, d, self.log)
        received = []
        upload.on_part_generated = received.append
        upload.upload('mybucket', 'mykey', '', {}, amz_headers)
        def eb(why):
            self.assertEquals(len(self.delayed_calls), 13)
            return why
        d.addErrback(eb)
        return self.assertFailure(d, UploadPartError)

    def test_upload_on_complete_error_recovery(self):
        client = ErroringFakeS3Client()
        parts_generator = DummyPartsGenerator()
        part_handler = DummyPartHandler()
        counter = PartsTransferredCounter('?')
        d = Deferred()
        received = []
        amz_headers = {'acl': 'public-read'}
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            counter, d, self.log)
        def check(task):
            self.flushLoggedErrors()
            self.assertEqual(len(self.delayed_calls), 3)
            self.assertIdentical(task, upload)
            self._assertPartsCompleted(parts_generator, part_handler,
                                      received, task, client)
        d.addCallback(check)
        received = []
        upload.on_part_generated = received.append
        upload.upload('mybucket', 'mykey', '', {}, amz_headers)
        return d

    def test_upload_on_complete_error_timeout_finally(self):
        client = ErroringFakeS3Client(100)
        parts_generator = DummyPartsGenerator()
        part_handler = DummyPartHandler()
        counter = PartsTransferredCounter('?')
        d = Deferred()
        received = []
        amz_headers = {'acl': 'public-read'}
        upload = MultipartUpload(client, None, parts_generator, part_handler,
            counter, d, self.log)
        received = []
        upload.on_part_generated = received.append
        upload.upload('mybucket', 'mykey', '', {}, amz_headers)
        def eb(why):
            self.assertEquals(len(self.delayed_calls), 13)
            return why
        d.addErrback(eb)
        return self.assertFailure(d, ValueError)

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
            self.assertEqual(task.amz_headers, {'acl': 'public-read'})
            self.assert_(self.log.buffer)
            verifyObject(IPartsGenerator, task.parts_generator)
            verifyObject(IPartHandler, task.part_handler)
            for entry in self.log.buffer:
                self.assertEqual(entry[0], 'msg')
        manager = MultipartUploadsManager(log=self.log)
        fd = StringIO("some data")
        d = manager.upload(fd, 'mybucket', 'mykey',
                           amz_headers={'acl':'public-read'})
        d.addCallback(check)
        return d
