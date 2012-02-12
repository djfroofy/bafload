from zope.interface import implements

from bafload.interfaces import ILog

class FakeLog(object):
    implements(ILog)

    def __init__(self):
        self.buffer = []

    def msg(self, *message, **kw):
        self.buffer.append(('msg', message, kw))

    def err(self, stuff, why, **kw):
        self.buffer.append(('err', stuff, why, kw))


