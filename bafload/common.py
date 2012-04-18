from zope.interface import implements

from twisted.python import log as twisted_log

from bafload.interfaces import ITransmissionCounter, ILog


class ProgressLoggerMixin(object):
    log = twisted_log

    def set_log(self, log=None):
        if log is None:
            return
        self.log = ILog(log)


class BaseCounter(ProgressLoggerMixin):
    implements(ITransmissionCounter)

    completed = 0
    expected = None
    receiving = None
    context = ''
    format_for_stdout = True

    def __init__(self, expected):
        self.expected = expected

    def increment_count(self):
        self.completed += 1
        verb = ('transferred', 'received')[self.receiving]
        cr = ('', '\r')[self.format_for_stdout]
        self.log.msg('%-40s %s' % ('%s%s parts %d/%s' % (self.context, verb,
            self.completed, self.expected), cr))
