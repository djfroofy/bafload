from collections import deque

from zope.interface import implements

from twisted.internet.defer import Deferred

from bafload.interfaces import IThrottler


class PassThruThrottler(object):
    implements(IThrottler)

    def throttle(self, func, *args, **kwargs):
        return func(*args, **kwargs)


class MaxConcurrentThrottler(object):
    implements(IThrottler)

    def __init__(self, max=10):
        self.max = max
        self.pending = 0
        self._backlog = deque()

    def throttle(self, func, *args, **kwargs):
        if self.pending >= self.max:
            d = Deferred()
            self._backlog.append((d, func, args, kwargs))
        else:
            d = self._do_call(func, args, kwargs)
        self.pending += 1
        return d

    def _do_call(self, func, args, kwargs):
        d = func(*args, **kwargs)
        d.addBoth(self._finish_call)
        return d

    def _finish_call(self, passthru):
        self.pending -= 1
        if self._backlog:
            (d, func, args, kwargs) = self._backlog.popleft()
            self._do_call(func, args, kwargs).chainDeferred(d)
        return passthru
