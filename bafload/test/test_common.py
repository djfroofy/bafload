from zope.interface import implements
from zope.interface.verify import verifyClass, verifyObject

from twisted.trial.unittest import TestCase

from bafload.interfaces import ITransmissionCounter, ILog
from bafload.common import BaseCounter

class InterfacesTestCase(TestCase):

    def test_base_counter_ifaces(self):
        verifyClass(ITransmissionCounter, BaseCounter)
        counter = BaseCounter(365)
        verifyObject(ITransmissionCounter, counter)


class FakeLog(object):
    implements(ILog)

    def __init__(self):
        self.buffer = []

    def msg(self, *message, **kw):
        self.buffer.append(('msg', message, kw))

    def err(self, stuff, why, **kw):
        self.buffer.append(('err', stuff, why, kw))


class CounterTestCase(TestCase):

    def setUp(self):
        super(CounterTestCase, self).setUp()
        self.log = FakeLog()
        self.counter = BaseCounter(365)
        self.counter.set_log(self.log)

    def test_receiving_verb(self):
        self.counter.receiving = True
        self.counter.increment_count()
        b = self.log.buffer
        self.assertEquals(b, [('msg', ('received parts 1/365                  '
                                       '   \r',), {})])

    def test_not_receiving_verb(self):
        self.counter.receiving = False
        self.counter.increment_count()
        b = self.log.buffer
        self.assertEquals(b, [('msg', ('transferred parts 1/365                '
                                       '  \r',), {})])

    def test_not_formatted_for_stdout(self):
        self.counter.receiving = True
        self.counter.format_for_stdout = False
        self.counter.increment_count()
        b = self.log.buffer
        self.assertEquals(b, [('msg', ('received parts 1/365                  '
                                       '   ',), {})])

    def test_incrementing(self):
        b = self.log.buffer
        next_msg = lambda : b[-1][1][0]
        self.counter.receiving = True
        self.counter.increment_count()
        self.assertIn('1/365', next_msg())
        self.counter.increment_count()
        self.assertIn('2/365', next_msg())
        self.counter.increment_count()
        self.assertIn('3/365', next_msg())
