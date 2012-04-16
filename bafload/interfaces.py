# Copryright 2012 Drew Smathers, See LICENSE
from zope.interface import Interface, Attribute, directlyProvides

from twisted.python import log


class ILog(Interface):
    """
    API for logging components that follow
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
    A Parts generates generates parts and part numbers for
    each range of parts.
    """

    part_size = Attribute("Part size. For S3 this should be no smaller "
                          "than 5MB.")

    def generate_parts(fd):
        """
        This method should generate or return iternable for parts from
        a file-like object. Each item generated should be a 2-tuple:
        (parts, part_number)

        @param fd: file-like object to read parts from
        """

    def count_parts(fd):
        """
        Optionally return number of parts for the given file descriptor.
        The string "?" should be returned if this cannot be determined or
        for whatever other reason.
        """


class IPartHandler(IProgressLogger):
    """
    Part Handler receives parts and sequences number and uploads
    or dispatches to another uploader component.
    """

    bucket = Attribute("S3 Bucket name")
    object_name = Attribute("S3 Key")
    upload_id = Attribute("The multipart upload id")

    def handle_part(part, part_number):
        """
        Handle a part, uploading to s3 or dispatching to process to
        do upload etc. This should fire with a 2-tuple (etag, part_number)

        @param part: The part as bytes C{str} or L{IBodyProducer} (Must be
                     something adaptable to L{IBodyProducer})
        @param part_number: The part number for the part
        """


class IMultipartUploadsManager(IProgressLogger):
    """
    A component which manages S3 Multipart uploads
    """

    def upload(fd, bucket, object_name, content_type=None, metadata={},
               parts_generator=None, part_handler=None):
        """
        @param fd: A file-like object to read parts from
        @param bucket: The bucket name
        @param object_name: The object name / key
        @param content_type: The Content-Type
        @param metadata: C{dict} of metadata for amz- headers
        @param parts_generator: provider of L{IPartsGenerator} (if not
            specified this method should use some default provider)
        @param part_handler: provider of L{IPartHandler} (if not specified,
            this method should use some default provider)
        """

class IThrouputCounter(Interface):
    """
    An API for tracking average throughput.
    """

    def start_entity(id):
        """
        Mark current time as start of upload for entity C{id}.
        """

    def stop_entity(id, size):
       """
       Mark current time as end of upload of entity C{id} where tranferred
       bytes count is C{size}.
       """

    def read():
        """
        Read time series data in the form:

        [(t_0, v_0), (t_1, v_1), ... (t_n, v_n)]
        """


class IByteLength(Interface):
    """
    An integral byte count.
    """


class IFile(Interface):
    """
    Stub interface for file objects.
    """

class IStringIO(Interface):
    """
    Stub interface for StringIO objects.
    """
