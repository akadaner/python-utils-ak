from datetime import datetime
from typing import Callable, Union

from utils_ak.architecture import PrefixHandler
from sortedcollections import SortedList

from utils_ak.loguru import configure_loguru
from utils_ak.time import cast_ts
from loguru import logger


class SimpleEventManager:
    """Add and subscribe to events"""

    def __init__(self):
        self.events = SortedList(key=lambda v: v[1])  # sorted(topic, ts, event)])
        self.prefix_handler = PrefixHandler()  # run events on topics with the specified prefix
        self.last_ts = None

    def subscribe(self, topic: str, callback: Callable):
        self.prefix_handler.add(topic, callback)

    def add_event(self, topic: str, ts: float, event: dict, duplicates_allowed: bool = False) -> Union[bool, tuple]:
        # - If duplicates are not allowed, return if event is already present

        if not duplicates_allowed and self.is_event_present(topic, ts, event):
            return False

        # - Add event to the list

        self.events.add((topic, ts, event))

        # - Return event

        return topic, ts, event

    def is_event_present(self, topic: str, ts: float, event: dict, ts_error: float = 1e-5):
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

    def run(self, skip_old_events: bool = False):
        while True:
            # - Return if no events left

            if not self.events:
                return

            # - Get the next event

            topic, ts, event = self.events.pop(0)

            # - Log warning if event is older than the last event

            if self.last_ts is not None and ts < self.last_ts:
                if skip_old_events:
                    logger.warning("Old event was added to the events timeline, skipping")
                    continue
                else:
                    logger.warning("Old event was added to the events timeline, processing it anyway")

            # - Process event with prefix handler

            self.prefix_handler(topic, ts, event)  # todo later: switch to kwargs

            # - Update last ts

            self.last_ts = max(ts, self.last_ts or 0)


def test():
    # - Configure logging

    configure_loguru()

    # - Init event manager

    event_manager = SimpleEventManager()

    # - Init counter stateful object

    class Counter:
        def __init__(self):
            self.value = 0

        def on_count(
            self,
            topic: str,
            ts: float,
            event: dict,
        ):
            logger.info("On count, before", topic=topic, ts=ts, event=event, value=self.value)
            self.value += event["num"]
            logger.info("On count, after ", topic=topic, ts=ts, event=event, value=self.value)

    counter = Counter()

    # - Subscribe to count

    event_manager.subscribe("count", counter.on_count)

    # - Spawn a couple of simple events

    now_ts = cast_ts(datetime.now())
    event_manager.add_event("count.up", now_ts, {"num": 3})
    event_manager.add_event("count.down", now_ts + 2, {"num": -11})

    # - Test event present

    assert event_manager.is_event_present("count.up", now_ts, {"num": 3})
    assert event_manager.is_event_present("count.up", now_ts + 1e-10, {"num": 3})
    assert event_manager.is_event_present("count.up", now_ts - 1e-10, {"num": 3})
    assert not event_manager.is_event_present("count.up", now_ts + 1, {"num": 3})
    assert not event_manager.is_event_present("different topic", now_ts + 1e-10, {"num": 3})
    assert not event_manager.is_event_present("count.up", now_ts + 1e-10, {"different event": 10})

    # - Test duplicate addition

    events_count_before = len(event_manager.events)
    event_manager.add_event("count.up", now_ts, {"num": 3})
    assert len(event_manager.events) == events_count_before

    # - Run event manage

    event_manager.run()


if __name__ == "__main__":
    test()
