import numpy as np

import uuid

from utils_ak.dag import *
from utils_ak.numeric import custom_round
from utils_ak.time import *
from utils_ak.serialization import cast_js

ERROR = 1e-5


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


class CableMixin:
    def cable(self, orient):
        assert orient in ['in', 'out']
        if orient == 'in':
            nodes = self.parents
        elif orient == 'out':
            nodes = self.children

        if not nodes:
            return
        else:
            assert len(nodes) == 1
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


class Container(Actor, CableMixin):
    def __init__(self, id=None, max_pressure_out=None):
        super().__init__(id)
        self.value = 0
        self.max_pressure_out = max_pressure_out

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self.value += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        if self.cable('out'):
            input_speed = self.speed('in')

            if abs(self.value) < ERROR:
                self.cable('out').pressure_in = min(self.max_pressure_out, input_speed)
            else:
                self.cable('out').pressure_in = self.max_pressure_out

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        if self.value > ERROR and self.speed('drain') < -ERROR:
            eta = self.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Container {self.id}'

    def stats(self):
        return {'value': self.value}


class Cable(Actor):
    def __init__(self, id=None):
        super().__init__(id)
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

    def update_speed(self, ts):
        pressures = [self.pressure_in, self.pressure_out]
        pressures = [p if p is not None else np.nan for p in pressures]
        if all(np.isnan(p) for p in pressures):
            raise Exception('No pressures specified')
        self.current_speed = np.nanmin(pressures)

    def __str__(self):
        return f'Cable {self.id}'

    def stats(self):
        return {'current_speed': self.current_speed, 'pressure_in': self.pressure_in, 'pressure_out': self.pressure_out}


class ProcessingContainer(Actor, CableMixin):
    def __init__(self, id=None, processing_time=5, max_pressure_out=50):
        super().__init__(id)
        self.processing_time = processing_time

        self._container_in = Container()
        self._cable = Cable()
        self._cable.pressure_in = 0
        self._container_out = Container()
        connect(self._container_in, self._cable)
        connect(self._cable, self._container_out)

        self.max_pressure_out = max_pressure_out

        self.last_cable_speed = None

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self._container_in.value += (ts - self.last_ts) * self.speed('in')
        self._container_in.value -= (ts - self.last_ts) * self._cable.current_speed
        self._container_out.value += (ts - self.last_ts) * self._cable.current_speed
        self._container_out.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        # set out pressure
        if self.cable('out'):
            if abs(self._container_out.value) < ERROR:
                self.cable('out').pressure_in = min(self.max_pressure_out, self._cable.current_speed)
            else:
                self.cable('out').pressure_in = self.max_pressure_out

    def update_speed(self, ts):
        self._cable.update_speed(ts)
        if self.last_cable_speed != self.speed('in'):
            self.add_event('processing_container.set_pressure', ts + self.processing_time, {'pressure': self.speed('in')})
            self.last_cable_speed = self.speed('in')

    def on_set_pressure(self, topic, ts, event):
        self._cable.pressure_in = event['pressure']

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        speed_drain = self._cable.current_speed - self.speed('out')

        if self._container_out.value > ERROR and speed_drain < -ERROR:
            eta = self._container_out.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Processing Container: {self.id}'

    def stats(self):
        return {'container_in': self._container_in.stats(), 'cable': self._cable.stats(), 'container_out': self._container_out.stats()}

    def subscribe(self):
        self.event_manager.subscribe('processing_container.set_pressure', self.on_set_pressure)
