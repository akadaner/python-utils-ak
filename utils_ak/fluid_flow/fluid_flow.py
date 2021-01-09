import numpy as np

from utils_ak.dag import *
from utils_ak.simple_event_manager import *

ERROR = 1e-5
EVENT_MANAGER = SimpleEventManager()


class Actor(DAGNode):
    def __init__(self):
        super().__init__()
        self.last_ts = None

    def update_last_ts(self, ts):
        self.last_ts = ts


class Container(Actor):
    def __init__(self):
        super().__init__()
        self.value = 0

    def cable(self, orient):
        assert orient in ['in', 'out']
        if orient == 'in':
            nodes = self.parents
        elif orient == 'out':
            nodes = self.children

        if not nodes:
            return
        else:
            return nodes[0]

    def speed(self, speed_type):
        assert speed_type in ['in', 'out', 'drain']
        if speed_type == 'in':
            if not self.cable('in'):
                return 0
            return self.cable('in').current_speed
        elif speed_type == 'out':
            if not self.cable('out'):
                return 0
            return self.cable('out').current_speed
        elif speed_type == 'drain':
            return self.speed('in') - self.speed('out')

    def update_value(self, ts):
        self.value += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        if self.cable('out'):
            input_speed = self.speed('in')

            if self.value == 0:
                self.cable('out').pressure_in = input_speed
            else:
                self.cable('out').pressure_in = None  # infinite speed allowed

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        if self.speed('drain') < 0:
            eta = self.value / self.speed('drain')
            EVENT_MANAGER.add_event(ts + eta, 'update.trigger', {})


class Cable(Actor):
    def __init__(self):
        super().__init__()
        self.current_speed = 0
        self.pressure_in = None
        self.pressure_out = None

    @property
    def parent(self):
        if not self.parents:
            return
        assert len(self.parents) == 1
        return self.parents[0]

    @property
    def child(self):
        if not self.children:
            return
        assert len(self.children) == 1
        return self.children[0]

    def update_speed(self):
        pressures = [self.pressure_in, self.pressure_out]
        pressures = [p if p is not None else np.nan for p in pressures]
        if all(p == np.nan for p in pressures):
            raise Exception('No pressures specified')
        self.current_speed = np.nanmin(pressures)

