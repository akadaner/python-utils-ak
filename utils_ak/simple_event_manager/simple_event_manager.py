from utils_ak.architecture import PrefixHandler
from sortedcollections import SortedList
import logging


class SimpleEventManager:
    def __init__(self):
        self.events = SortedList(key=lambda v: v[1])  # sorted(topic, ts, event)])
        self.prefix_handler = PrefixHandler()
        self.last_ts = 0

    def subscribe(self, topic, callback):
        self.prefix_handler.add(topic, callback)

    def add_event(self, topic, ts, event):
        self.events.add((topic, ts, event))

    def run(self):
        while True:
            if not self.events:
                return
            topic, ts, event = self.events.pop(0)

            if self.last_ts and ts < self.last_ts:
                logging.warning('Old event was added to the events timeline')
            self.prefix_handler(topic, ts, event)
            self.last_ts = max(ts, self.last_ts)


if __name__ == '__main__':
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
    em.add_event('count.up', cast_ts(datetime.now()), {'num': 3})
    em.add_event('count.down', cast_ts(datetime.now()) + 2, {'num': -11})
    em.run()
