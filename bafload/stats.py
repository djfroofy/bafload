import math
from collections import deque

from twisted.internet import reactor


class SlidingStats(object):
    """
    A sliding view of time a single floating point where time is descretely
    divided into slots of duration C{slot_duration_secs} and number of slots is
    C{size}. This should be initialized for a given time C{t} (float seconds)
    provided by the wizard. The window also has the property that there are no
    gaps between each time slot.
    """

    def __init__(self, t, slot_duration_secs=1, size=2048):
        self.slot_duration_secs = slot_duration_secs
        self.size = size
        self.generation = size * self.slot_duration_secs
        t = t - (t % self.slot_duration_secs)
        self._reset_slots(t)

    def _reset_slots(self, t):
        dur = self.slot_duration_secs
        n = self.size
        t0 = dur + t - self.generation
        self.slots = deque([(t0 + (i * dur), 0) for i in xrange(n)], n)

    def update(self, t, count):#, current=False):
        """
        Add C{count} the count at the base of time t (t') where t' is given by:
        
        t' == t  - t % slot_duration_secs

        If t' is older than our earliest time in the window this is a noop. If
        t' is newer than than our most current time (t_k) then slots t_k
        through t_n are added where t_n == t' - slot_duration_secs.

        @param t: The time to update for as C{float}
        @param count: The value to add to current total for t' (if in window)
        """
        tp = t - (t % self.slot_duration_secs)
        if tp < self.slots[0][0]:
            return
        if (tp - self.slots[-1][0]) > self.generation:
            self._reset_slots(tp)
        index = -1
        (last, v) = self.slots[index]
        while tp > last:
            self.slots.append((last + self.slot_duration_secs, 0))
            (last, v) = self.slots[index]
        if tp < last:
            index = int(((tp - last) / self.slot_duration_secs) - 1)
        tk, v = self.slots[index]
        self.slots[index] = (tp, v + count)


class ThroughputCounter(object):
    cutoff = 3600

    def __init__(self, clock=None):
        if clock is None:
            clock = reactor
        self.stats = SlidingStats(clock.seconds())
        self.starts = {}
        self.clock = clock

    def start_entity(self, id):
        t = self.clock.seconds()
        self.starts[id] = t

    def stop_entity(self, id, size):
        t1 = self.clock.seconds()
        t0 = self.starts.pop(id)
        total = t1 - t0
        if total < 5:
            self.stats.update(t1, size)
        else:
            for i in xrange(int(math.ceil(total / 5.))):
                t_k = t1 - (i * 5.)
                self.stats.update(t_k, size)

    def read(self):
        self.stats.update(time.time(), 0)
        return [s[1] for s in self.stats.slots]
