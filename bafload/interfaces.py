# Copryright 2012 Drew Smathers, See LICENSE

from zope.interface import Interface, Attribute, directlyProvides

from twisted.python import log


class ILog(Interface):
    """
    Stub interface for logging components that follow
    Twisted's log interface: msg() and err().
    """

    def msg(*message, **kw):
        """
        Write message.
        """

    def err(stuff, why, **kw):
        """
        Write failure.
        """

directlyProvides(log, ILog)


class IProgressLogger(Interface):

    def set_log(log):
        """
        Set logging component or provider of L{ILog}.
        """

class ITransmissionCounter(IProgressLogger):

    completed = Attribute("Number of parts/chunks transfered (starts at 0)")
    receiving = Attribute("If True, we're receiving data, otherwise we're "
                          "sending")

    def increment_count():
        """
        Count a part as completed, incrementing completed count.
        """

class IPartsGenerator(IProgressLogger):
    """
    A Parts generates generates bytes and sequence numbers for
    each range of bytes.
    """

    def generate_parts(fd):
        """
        This method should generate or return iternable for parts from
        a file-like object. Each item generated should be a 2-tuple:
        (bytes, seq_no)

        @param fd: file-like object to read parts from
        """

class IPartHandler(IProgressLogger):
    """
    Part Handler receives bytes and sequences number and uploads
    or dispatches to another uploader component.
    """

    bucket = Attribute("S3 Bucket name")
    object_name = Attribute("S3 Key")
    upload_id = Attribute("The multipart upload id")

    def handle_part(bytes, seq_no):
        """
        Handle a part, uploading to s3 or dispatching to process to
        do upload etc. This should fire with a 2-tuple (etag, seq_no)

        @param bytes: The raw bytes as C{str} to handle
        @param seq_no: The sequence number for the part
        """


class IMultipartUploadsManager(IProgressLogger):
    """
    A component which manages S3 Multipart uploads
    """

    def upload(fd, bucket, object_name, content_type=None, metadata={},
               parts_generator=None, part_handler=None):
        """
        @param fd: A file-like object to read bytes from
        @param bucket: The bucket name
        @param object_name: The object name / key
        @param content_type: The Content-Type
        @param metadata: C{dict} of metadata for amz- headers
        @param parts_generator: provider of L{IPartsGenerator} (if not
            specified this method should use some default provider)
        @param part_handler: provider of L{IPartHandler} (if not specified,
            this method should use some default provider)
        """

