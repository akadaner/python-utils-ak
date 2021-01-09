import numpy as np

import uuid

from utils_ak.dag import *
from utils_ak.simple_event_manager import *
from utils_ak.numeric import custom_round
from utils_ak.time import *

ERROR = 1e-5
EVENT_MANAGER = SimpleEventManager()


class Actor(DAGNode):
    def __init__(self, id=None):
        super().__init__()
        self.last_ts = None
        self.id = id or str(uuid.uuid4())

    def update_last_ts(self, ts):
        self.last_ts = ts


class Container(Actor):
    def __init__(self, id=None):
        super().__init__(id)
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

    def __str__(self):
        return f'Container {self.id}'


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

    def update_speed(self):
        pressures = [self.pressure_in, self.pressure_out]
        pressures = [p if p is not None else np.nan for p in pressures]
        if all(p == np.nan for p in pressures):
            raise Exception('No pressures specified')
        self.current_speed = np.nanmin(pressures)

    def __str__(self):
        return f'Cable {self.id}'


class PrimitiveFlow:
    def __init__(self):
        container1 = Container()
        container1.value = 100
        container2 = Container()
        cable = Cable()

        connect(container1, cable)
        connect(cable, container2)

        self.root = container1

    def print_current_flow(self):
        for node in self.root.iterate('down'):
            if isinstance(node, Container):
                print(cast_datetime(node.last_ts), node, node.value)
            elif isinstance(node, Cable):
                print(cast_datetime(node.last_ts), node, node.pressure_in, node.pressure_out, node.current_speed)

    def update(self, ts, topic, event):
        print('Processing time', ts)

        print('Updating value')
        for node in self.root.iterate('down'):
            getattr(node, 'update_value', lambda ts: None)(ts)
        self.print_current_flow()

        print('Updating pressure')
        for node in self.root.iterate('down'):
            getattr(node, 'update_pressure', lambda ts: None)(ts)
        self.print_current_flow()

        print('Updating speed')
        for node in self.root.iterate('down'):
            getattr(node, 'update_speed', lambda ts: None)(ts)
        self.print_current_flow()

        print('Updating triggers')
        for node in self.root.iterate('down'):
            getattr(node, 'update_triggers', lambda ts: None)(ts)
        self.print_current_flow()

        print('Updating last ts')
        for node in self.root.iterate('down'):
            getattr(node, 'update_last_ts', lambda ts: None)(ts)
        self.print_current_flow()


flow = PrimitiveFlow()
EVENT_MANAGER.subscribe('update', flow.update)
today_ts = int(customs_round(cast_ts(datetime.now()), 24 * 3600))
# EVENT_MANAGER.add_event(today_ts, 'update', {})
# EVENT_MANAGER.run()