from utils_ak.architecture import PrefixHandler
from sortedcollections import SortedList
import logging

class SimpleEventManager:
    def __init__(self):
        self.events = SortedList(key=lambda v: v[1])  # sorted(topic, ts, event)])
        self.prefix_handler = PrefixHandler()
        self.last_ts = None

    def subscribe(self, topic, callback):
        self.prefix_handler.add(topic, callback)

    def add_event(self, topic, ts, event, duplicates_allowed=False):
        if not duplicates_allowed:
            if self.is_event_present(topic, ts, event):
                return False
        self.events.add((topic, ts, event))
        return True

    def is_event_present(self, topic, ts, event, ts_error=1e-5):
        if not self.events:
            return False

        ind = self.events.bisect_left([topic, ts, event])

        for increment in [1, -1]:
            cur_ind = ind
            while True:
                if cur_ind < 0 or cur_ind >= len(self.events):
                    break
                cur_event = self.events[cur_ind]

                if increment == 1 and cur_event[1] > ts + ts_error:
                    break
                if increment == -1 and cur_event[1] < ts - ts_error:
                    break

                if cur_event[0] == topic and abs(cur_event[1] - ts) < ts_error and cur_event[2] == event:
                    return True

                cur_ind += increment

        return False

    def run(self):
        while True:
            if not self.events:
                return
            topic, ts, event = self.events.pop(0)

            if self.last_ts is not None and ts < self.last_ts:
                logging.warning('Old event was added to the events timeline')
            self.prefix_handler(topic, ts, event)
            self.last_ts = max(ts, self.last_ts or 0)


def test_simple_event_manager():
    em = SimpleEventManager()

    class Counter:
        def __init__(self):
            self.counter = 0

        def on_count(self, topic, ts, event):
            self.counter += event['num']
            print('Current counter', topic, ts, event, self.counter)
    counter = Counter()

    em.subscribe('count', counter.on_count)

    from utils_ak.time import cast_ts
    from datetime import datetime
    now_ts = cast_ts(datetime.now())
    em.add_event('count.up', now_ts, {'num': 3})
    em.add_event('count.down', now_ts + 2, {'num': -11})

    # test is_event_prent
    assert em.is_event_present('count.up', now_ts, {'num': 3})
    assert em.is_event_present('count.up', now_ts + 1e-10, {'num': 3})
    assert em.is_event_present('count.up', now_ts - 1e-10, {'num': 3})
    assert not em.is_event_present('count.up', now_ts + 1, {'num': 3})
    assert not em.is_event_present('different topic', now_ts + 1e-10, {'num': 3})
    assert not em.is_event_present('count.up', now_ts + 1e-10, {'different event': 10})

    # test duplicate addition
    events_count_before = len(em.events)
    em.add_event('count.up', now_ts, {'num': 3})
    assert len(em.events) == events_count_before

    em.run()


if __name__ == '__main__':
    test_simple_event_manager()
