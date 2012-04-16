from twisted.trial.unittest import TestCase
from twisted.python.failure import Failure
from twisted.internet.defer import succeed, fail

from bafload.test.util import FakeClock, FakeLog
from bafload.retry import BinaryExponentialBackoff


class BinaryExponentialBackoffTestCase(TestCase):

    def setUp(self):
        self.clock = FakeClock()
        self.log = FakeLog()
        self.algo = BinaryExponentialBackoff(
                clock=self.clock,
                log=self.log)

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
            self.assertNErrorsLogged(9)
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
            self.assertNErrorsLogged(9)
            self.assertEquals(result, ('a', 45))
        d = self.algo.retry(fail_9_times, 'a', k=45)
        d.addCallback(check)
        return d

    def test_retry_fail(self):
        def keep_failing(a, k=0):
            return fail(Failure(ValueError('woops')))
        def eb(why):
            self.assertEquals(len(self.clock.calls), 11)
            self.assertNErrorsLogged(12)
            return why
        d = self.algo.retry(keep_failing, 'a', k=45)
        d.addErrback(eb)
        return self.assertFailure(d, ValueError)

    def assertNErrorsLogged(self, n):
        error_ct = len([e for e in self.log.buffer if e[0] == 'err'])
        self.assertEquals(error_ct, n)

