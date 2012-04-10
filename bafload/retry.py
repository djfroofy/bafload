"""
Retry alogorithms:

    BinaryExponentialBackoff
"""
import random

from zope.interface import Interface, implements

from twisted.internet.defer import Deferred


__all__ = ['IRetryDeferred', 'BinaryExponentialBackoff']


class IRetryDeferred(Interface):

    def retry(f, *args, **kwargs):
        """
        Try calling a Deferred-returning until it succeeds or we give up
        and fail.
        """


class BinaryExponentialBackoff(object):
    implements(IRetryDeferred)

    scatter = random.random

    def __init__(self, fail_on_truncate=True, max_slots=12, slot_duration=0.1,
                 clock=None):
        self.fail_on_truncate = fail_on_truncate
        self.max_slots = max_slots
        self.slot_duration = slot_duration
        if clock is None:
            from twisted.internet import reactor as clock
        self.clock = clock

    def retry(self, f, *args, **kwargs):
        finished = Deferred()
        self._retry(None, finished, 0, f, args, kwargs)
        return finished

    def _retry(self, why, finished, count, f, a, kw):
        count = min(count, self.max_slots)
        if count == self.max_slots and self.fail_on_truncate:
            finished.errback(why)
            return
        def retry():
            d = f(*a, **kw)
            d.addCallback(finished.callback)
            d.addErrback(self._retry, finished, count + 1, f, a, kw)
        if count:
            when = self.slot_duration * ((2**count) - 1) * self.scatter()
            self.clock.callLater(when, retry)
        else:
            retry()
