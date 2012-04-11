import time
from collections import deque

class SlidingStats(object):

    def __init__(self, t, slot_duration_secs=5, size=1024):
        self.slot_duration_secs = slot_duration_secs
        self.size = size
        self.generation = size * self.slot_duration_secs
        t = t - (t % self.slot_duration_secs)
        self._reset_slots(t)

    def _reset_slots(self, t):
        dur = self.slot_duration_secs
        size = self.size
        start = dur + t - self.generation
        self.slots = deque([(start + (i * dur), 0) for i in xrange(size)])

    def update(self, t, count, current=False):
        t = t - (t % self.slot_duration_secs)
        if current and (t - self.slots[-1][0]) > self.generation:
            self._reset_slots(t)
        if current:
            while (t - self.slots[0][0]) > self.generation:
                self._extend()
            while (t - self.slots[-1][0]) >= self.slot_duration_secs:
                self._extend()
        index = -1
        (last, v) = self.slots[index]
        while t < last:
            index -= 1
            try:
                (last, v) = self.slots[index]
            except IndexError:
                return
        self.slots[index] = (t, v + count)

    def _extend(self):
        last = self.slots[-1][0]
        self.slots.append((last + self.slot_duration_secs, 0))
        self.slots.popleft()


class ThroughputCounter(object):
    cutoff = 3600

    def __init__(self):
        self.starts = {}

    def start_entity(self, id):
        t = time.time()
        self.starts[id] = t

    def stop_entity(self, id, size):
        t1 = time.time()
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
