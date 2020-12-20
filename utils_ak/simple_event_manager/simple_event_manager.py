from utils_ak.architecture import PrefixHandler
from sortedcollections import ItemSortedDict


class SimpleEventManager:
    def __init__(self):
        self.events = ItemSortedDict(lambda k, v: k, {})  # sorted(ts: [topic, event])])
        self.prefix_handler = PrefixHandler()

    def subscribe(self, topic, callback):
        self.prefix_handler.add(topic, callback)

    def add_event(self, topic, ts, event):
        self.events[ts] = [topic, event]

    def run(self):
        while True:
            if not self.events:
                return
            ts, (topic, event) = self.events.popitem(0)
            self.prefix_handler(topic, ts, event)


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
