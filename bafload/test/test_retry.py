from twisted.trial.unittest import TestCase
from twisted.python.failure import Failure
from twisted.internet.defer import succeed, fail

from bafload.test.util import FakeClock
from bafload.retry import BinaryExponentialBackoff


class BinaryExponentialBackoffTestCase(TestCase):

    def setUp(self):
        self.clock = FakeClock()
        self.algo = BinaryExponentialBackoff(clock=self.clock)

    def test_retry(self):
        fails = []
        self.algo.slot_duration = 1
        self.algo.scatter = lambda : 1
        def fail_9_times(a, k=0):
            if len(fails) == 9:
                return succeed((a, k))
            fails.append(1)
            return fail(Failure(ValueError('woops')))
        def check(result):
            times = [c[0] for c in self.clock.calls]
            self.assertEquals(times,
                    [1, 3, 7, 15, 31, 63, 127, 255, 511])
            self.assertEquals(result, ('a', 45))
        d = self.algo.retry(fail_9_times, 'a', k=45)
        d.addCallback(check)
        return d

    def test_retry_random(self):
        fails = []
        def fail_9_times(a, k=0):
            if len(fails) == 9:
                return succeed((a, k))
            fails.append(1)
            return fail(Failure(ValueError('woops')))
        def check(result):
            self.assertEquals(len(self.clock.calls), 9)
            self.assertEquals(result, ('a', 45))
        d = self.algo.retry(fail_9_times, 'a', k=45)
        d.addCallback(check)
        return d

    def test_retry_fail(self):
        fails = []
        def fail_12_times(a, k=0):
            if len(fails) == 12:
                return succeed((a, k))
            fails.append(1)
            return fail(Failure(ValueError('woops')))
        def eb(why):
            self.assertEquals(len(self.clock.calls), 11)
            return why
        d = self.algo.retry(fail_12_times, 'a', k=45)
        d.addErrback(eb)
        return self.assertFailure(d, ValueError)
