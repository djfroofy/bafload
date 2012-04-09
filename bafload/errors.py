
class UploadPartError(Exception):

    def __init__(self, root_exception, part, part_number):
        Exception.__init__(self, repr(root_exception))
        self.root_exception = root_exception
        self.part = part
        self.part_number = part_number


