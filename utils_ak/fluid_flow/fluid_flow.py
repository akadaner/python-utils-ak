import numpy as np

import uuid

from utils_ak.dag import *
from utils_ak.simple_event_manager import *
from utils_ak.numeric import custom_round
from utils_ak.time import *
from utils_ak.serialization import cast_js

ERROR = 1e-5
EVENT_MANAGER = SimpleEventManager()


class Actor(DAGNode):
    event_manager = EVENT_MANAGER
    def __init__(self, id=None):
        super().__init__()
        self.last_ts = None
        self.id = id or str(uuid.uuid4())

    def add_event(self, topic, ts, event):
        self.event_manager.add_event(topic, ts, event)

    def update_last_ts(self, ts):
        self.last_ts = ts


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
        self._container_out = Container()
        connect(self._container_in, self._cable)
        connect(self._cable, self._container_out)
        self.max_pressure_out = max_pressure_out

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self._container_in += (ts - self.last_ts) * self.speed('in')
        self._container_in -= (ts - self.last_ts) * self._cable.current_speed
        self._container_out += (ts - self.last_ts) * self._cable.current_speed
        self._container_out -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        # set out pressure
        if self.cable('out'):
            if abs(self._container_out.value) < ERROR:
                self.cable('out').pressure_in = min(self.max_pressure_out, self._cable.current_speed)
            else:
                self.cable('out').pressure_in = self.max_pressure_out

    def update_speed(self, ts):
        self._cable.update_speed(ts)
        self.add_event('processing_container.set_pressure', ts + self.processing_time, {'pressure': self._container_in.speed('in')})

    def on_set_pressure(self, topic, ts, event):
        print('Setting inner pressure in ProcessingContainer')
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

def test_primitive_flow():
    class PrimitiveFlow:
        def __init__(self):
            container1 = Container('Input', max_pressure_out=50)
            container1.value = 100
            container2 = Container('Ouput')
            cable = Cable('Cable')

            connect(container1, cable)
            connect(cable, container2)

            self.root = container1

        def __str__(self):
            values = ['Primitive Flow']
            for node in self.root.iterate('down'):
                values.append(' ' * 4 + str(node) + ': ' + cast_js(node.stats()))
            return '\n'.join(values)

        def update(self, topic, ts, event):
            print('Processing time', ts)

            print('Updating value')
            for node in self.root.iterate('down'):
                getattr(node, 'update_value', lambda ts: None)(ts)
            print(self)

            print('Updating pressure')
            for node in self.root.iterate('down'):
                getattr(node, 'update_pressure', lambda ts: None)(ts)
            print(self)

            print('Updating speed')
            for node in self.root.iterate('down'):
                getattr(node, 'update_speed', lambda ts: None)(ts)
            print(self)

            print('Updating triggers')
            for node in self.root.iterate('down'):
                getattr(node, 'update_triggers', lambda ts: None)(ts)
            print(self)

            print('Updating last ts')
            for node in self.root.iterate('down'):
                getattr(node, 'update_last_ts', lambda ts: None)(ts)
            print(self)
            print()

    import warnings
    warnings.filterwarnings("ignore")

    flow = PrimitiveFlow()
    EVENT_MANAGER.subscribe('', flow.update)
    # today_ts = int(custom_round(cast_ts(datetime.now()), 24 * 3600))
    EVENT_MANAGER.add_event('update', 0, {})
    EVENT_MANAGER.run()


def test_processing_flow():
    class ProcessingFlow:
        def __init__(self):
            container1 = Container('Input', max_pressure_out=50)
            container1.value = 100
            container2 = ProcessingContainer('Ouput')

            cable = Cable('Cable')

            connect(container1, cable)
            connect(cable, container2)

            self.root = container1

        def __str__(self):
            values = ['Primitive Flow']
            for node in self.root.iterate('down'):
                if isinstance(node, Container):
                    values.append(' ' * 4 + ', '.join([str(x) for x in [node.last_ts, node, node.value]]))
                elif isinstance(node, Cable):
                    values.append(' ' * 4 + ', '.join([str(x) for x in [node.last_ts, node, node.pressure_in, node.pressure_out, node.current_speed]]))
                elif isinstance(node, ProcessingContainer):
                    values.append(' ' * 4 + ', '.join([str(x) for x in [node.last_ts, node, node._container_in.value, ]]))
            return '\n'.join(values)

        def update(self, topic, ts, event):
            print('Processing time', ts)

            print('Updating value')
            for node in self.root.iterate('down'):
                getattr(node, 'update_value', lambda ts: None)(ts)
            print(self)

            print('Updating pressure')
            for node in self.root.iterate('down'):
                getattr(node, 'update_pressure', lambda ts: None)(ts)
            print(self)

            print('Updating speed')
            for node in self.root.iterate('down'):
                getattr(node, 'update_speed', lambda ts: None)(ts)
            print(self)

            print('Updating triggers')
            for node in self.root.iterate('down'):
                getattr(node, 'update_triggers', lambda ts: None)(ts)
            print(self)

            print('Updating last ts')
            for node in self.root.iterate('down'):
                getattr(node, 'update_last_ts', lambda ts: None)(ts)
            print(self)
            print()

    import warnings
    warnings.filterwarnings("ignore")

    flow = ProcessingFlow()
    EVENT_MANAGER.subscribe('', flow.update)
    # today_ts = int(custom_round(cast_ts(datetime.now()), 24 * 3600))
    EVENT_MANAGER.add_event('update', 0, {})
    EVENT_MANAGER.run()


if __name__ == '__main__':
    test_primitive_flow()