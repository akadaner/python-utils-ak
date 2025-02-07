import uuid
from typing import Optional

from utils_ak.dag import DAGNode


class Actor(DAGNode):
    def __init__(self, name: Optional[str] = None):
        super().__init__()

        self.last_ts = None
        self.name = name
        self.id = str(uuid.uuid4())
        self.event_manager = None

    def set_event_manager(self, event_manager):
        self.event_manager = event_manager
        for actor in self.inner_actors():
            actor.set_event_manager(event_manager)

    # - General overridable

    def inner_actors(self):
        return []

    def add_event(self, topic, ts, event):
        self.event_manager.add_event(topic, ts, event)

    def subscribe(self):
        for node in self.inner_actors():
            node.subscribe()

    def active_periods(self):
        return []

    def reset(self):
        self.last_ts = None
        for actor in self.inner_actors():
            actor.reset()

    def stats(self):
        return {}

    def display_stats(self):
        return {}

    # - Updaters

    def update_last_ts(self, ts):
        self.last_ts = ts
        for actor in self.inner_actors():
            actor.update_last_ts(ts)
