from zope.interface.verify import verifyClass, verifyObject

from twisted.trial.unittest import TestCase

from bafload.test.util import FakeClock
from bafload.interfaces import IThrouputCounter
from bafload.stats import SlidingStats, ThroughputCounter


class SlidingStatsTestCase(TestCase):

    def test_initial_slots(self):
        stats = SlidingStats(200.0, 2, 100)
        expected = map(float, range(2, 202, 2))
        expected = zip(expected, [0] * 100)
        self.assertEquals(list(stats.slots), expected)
        self.assertEquals(stats.size, 100)
        self.assertEquals(stats.slot_duration_secs, 2)
        self.assertEquals(stats.generation, 200)

    def test_update(self):
        stats = SlidingStats(400.0, 2, 100)
        stats.update(400, 5)
        stats.update(400, 14)
        window = map(float, range(202, 402, 2))
        expected = zip(window, [0] * 99 + [19])
        self.assertEquals(list(stats.slots), expected)
        stats.update(300., 6)
        stats.update(302.5, 8)
        stats.update(301.5, 4)
        expected = zip(window, ([0] * 49) + [10., 8.] + ([0] * 48) + [19])
        self.assertEquals(list(stats.slots), expected)
        stats.update(20., 200)
        self.assertEquals(list(stats.slots), expected)

    def test_reset_when_past_generation(self):
        stats = SlidingStats(200.0, 2, 100)
        stats.update(900, 10)
        window = map(float, range(702, 902, 2))
        expected = zip(window, [0] * 99 + [10])
        self.assertEquals(list(stats.slots), expected)

    def test_slide_on_update(self):
        stats = SlidingStats(200, 2, 100)
        stats.update(350., 7)
        window = map(float, range(152, 352, 2))
        expected = zip(window, [0] * 99 + [7])
        self.assertEquals(list(stats.slots), expected)

class ThroughputCounterTestCase(TestCase):

    def test_iface(self):
        verifyClass(IThrouputCounter, ThroughputCounter)
        verifyObject(IThrouputCounter, ThroughputCounter())

    def test_initialization(self):
        clock = FakeClock()
        clock.tick(200)
        counter = ThroughputCounter(clock)
        last = counter.stats.slots[-1][0]
        self.assertEquals(last, 200)

    def _build_counter(self):
        stats = SlidingStats(50, 1, 50)
        clock = FakeClock(50)
        return ThroughputCounter(clock, stats)

    def test_read(self):
        counter = self._build_counter()
        clock = counter.clock
        counter.start_entity('a')
        window = map(float, range(1, 51))
        expected = zip(window, [0] * 50)
        self.assertEquals(counter.read(), expected)
        clock.tick(5)
        counter.start_entity('b')
        clock.tick(15)
        counter.stop_entity('a', 40)
        window = map(float, range(21, 71))
        expected = zip(window, [0] * 30 + [2] * 20)
        self.assertEquals(counter.read(), expected)
        clock.tick(25)
        counter.stop_entity('b', 20)
        window = map(float, range(46, 96))
        expected = zip(window, [0] * 5 + [2] * 5 + [2.5] * 15 + [0.5] * 25)
        self.assertEquals(counter.read(), expected)

