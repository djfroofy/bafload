from zope.interface.verify import verifyObject, verifyClass

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred, gatherResults

from bafload.interfaces import IThrottler
from bafload.throttle import MaxConcurrentThrottler, PassThruThrottler


class TestFunc(object):

    def __init__(self):
        self.called = []
        self.finished = []

    def __call__(self, *args, **kw):
        self.called.append((args, kw))
        d = Deferred()
        self.finished.append(d)
        d.addBoth(self._remove_deferred, d)
        return d

    def _remove_deferred(self, passthru, d):
        self.finished.remove(d)
        return passthru


class PassThruThrottlerTestCase(TestCase):

    def test_iface(self):
        verifyClass(IThrottler, PassThruThrottler)
        verifyObject(IThrottler, PassThruThrottler())

    def test_throttle(self):
        test_func = TestFunc()
        throttler = PassThruThrottler()
        d = throttler.throttle(test_func, 'foo', a=1, b=2)
        self.assertEqual(test_func.called, [(('foo',), {'a': 1, 'b': 2})])
        d.callback('ok')
        def check(r):
            self.assertEqual(r, 'ok')
        return d.addCallback(check)


class MaxConcurrentThrottlerTestCase(TestCase):

    def test_iface(self):
        verifyClass(IThrottler, MaxConcurrentThrottler)
        verifyObject(IThrottler, MaxConcurrentThrottler())

    def test_throttle(self):
        test_func = TestFunc()
        throttler = MaxConcurrentThrottler(3)
        ds = []
        for i in range(5):
            ds.append(throttler.throttle(test_func, a=i))
        finished = gatherResults(list(ds))
        self.assertEqual(len(test_func.called), 3)
        d = ds.pop(0)
        d.callback(1)
        self.assertEqual(len(test_func.called), 4)
        ds.pop(0).callback(1)
        self.assertEqual(len(test_func.called), 5)
        self.assertEqual(throttler.pending, 3)
        for d in list(test_func.finished):
            d.callback(1)
        self.failIf(throttler.pending)
        def check(r):
            self.assertEqual(r, [1, 1, 1, 1, 1])
        return finished.addCallback(check)
