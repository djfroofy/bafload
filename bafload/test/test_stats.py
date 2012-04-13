from pprint import pformat

from twisted.trial.unittest import TestCase

from bafload.stats import SlidingStats

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
    pass

