"""
Registers adapters used by bafload
"""
import os
import types
from StringIO import StringIO

from zope.interface import classImplements

from twisted.python.components import registerAdapter

from bafload.interfaces import IByteLength, IFile, IStringIO


classImplements(int, IByteLength)
classImplements(types.FileType, IFile)
classImplements(StringIO, IStringIO)


def fd_to_bytelength(fd):
    """
    Adapt a File object to a byte length (int)
    """
    return os.fstat(fd.fileno()).st_size


registerAdapter(fd_to_bytelength, IFile, IByteLength)


def stringio_to_bytelength(stringio):
    """
    Adapt a StringIO object to a bytes length (int)
    """
    return stringio.len

registerAdapter(stringio_to_bytelength, IStringIO, IByteLength)
