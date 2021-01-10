import uuid
from utils_ak.dag import DAGNode


class Actor(DAGNode):
    def __init__(self, id=None):
        super().__init__()
        self.last_ts = None
        self.id = id or str(uuid.uuid4())
        self.event_manager = None

    def set_event_manager(self, event_manager):
        self.event_manager = event_manager

    def add_event(self, topic, ts, event):
        self.event_manager.add_event(topic, ts, event)

    def update_last_ts(self, ts):
        self.last_ts = ts

    def subscribe(self):
        pass

    def stats(self):
        return {}