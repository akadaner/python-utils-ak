from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin, Pipe
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import ERROR



class ProcessingContainer(Actor, PipeMixin):
    def __init__(self, id=None, item_in='default', item_out='default', processing_time=5, transformation_factor=1., max_pressure_out=50):
        super().__init__(id)
        self.processing_time = processing_time

        self._container_in = Container(item_in)
        self._pipe = Pipe()
        self._pipe.pressure_in = 0
        self._container_out = Container(item_out)
        connect(self._container_in, self._pipe)
        connect(self._pipe, self._container_out)

        self.max_pressure_out = max_pressure_out

        self.last_cable_speed = None
        self.transformation_factor = transformation_factor

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self._container_in.value += (ts - self.last_ts) * self.speed('in')
        self._container_in.value -= (ts - self.last_ts) * self._pipe.current_speed
        self._container_out.value += (ts - self.last_ts) * self._pipe.current_speed * self.transformation_factor
        self._container_out.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        # set out pressure
        if self.pipe('out'):
            if abs(self._container_out.value) < ERROR:
                self.pipe('out').pressure_in = min(self.max_pressure_out, self._pipe.current_speed)
            else:
                self.pipe('out').pressure_in = self.max_pressure_out

    def update_speed(self, ts):
        self._pipe.update_speed(ts)
        if self.last_cable_speed != self.speed('in'):
            self.add_event('processing_container.set_pressure', ts + self.processing_time, {'pressure': self.speed('in')})
            self.last_cable_speed = self.speed('in')

    def on_set_pressure(self, topic, ts, event):
        self._pipe.pressure_in = event['pressure']

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        speed_drain = self._pipe.current_speed - self.speed('out')

        if self._container_out.value > ERROR and speed_drain < -ERROR:
            eta = self._container_out.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Processing Container: {self.id}'

    def stats(self):
        return {'container_in': self._container_in.stats(), 'cable': self._pipe.stats(), 'container_out': self._container_out.stats()}

    def subscribe(self):
        self.event_manager.subscribe('processing_container.set_pressure', self.on_set_pressure)
