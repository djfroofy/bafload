from zope.interface import implements

from twisted.internet.defer import succeed

from txaws.s3.model import (MultipartInitiationResponse,
    MultipartCompletionResponse)

from bafload.interfaces import ILog


class FakeLog(object):
    implements(ILog)

    def __init__(self):
        self.buffer = []

    def msg(self, *message, **kw):
        self.buffer.append(('msg', message, kw))

    def err(self, stuff, why, **kw):
        self.buffer.append(('err', stuff, why, kw))


class FakeS3Client(object):

    def __init__(self):
        self.calls = []

    def init_multipart_upload(self, bucket, object_name, content_type,
                              metadata, amz_headers={}):
        self.calls.append(('init_multipart_upload', bucket, object_name,
            content_type, metadata, amz_headers))
        return succeed(MultipartInitiationResponse(bucket, object_name, '1234'))

    def upload_part(self, bucket, object_name, upload_id, part_number,
                    data=None, content_type=None, metadata={},
                    body_producer=None):
        return succeed({'ETag': '"0123456789"'})

    def complete_multipart_upload(self, bucket, object_name, upload_id,
                                  parts_list):
        return succeed(MultipartCompletionResponse(
            'http://%s.example.com/%s' % (bucket, object_name),
            bucket, object_name, '"0123456789"'))


