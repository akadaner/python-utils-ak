from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin, Pipe
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import ERROR
from utils_ak.fluid_flow.pressure import calc_minimum_pressure


class Processor(Actor, PipeMixin):
    def __init__(self, id=None, item_in='default', item_out='default',
                 processing_time=0,
                 processing_limit=None,
                 transformation_factor=1.,
                 max_pressure_in=None, max_pressure_out=None):
        super().__init__(id)
        self.processing_time = processing_time
        self.processing_limit = processing_limit
        self.transformation_factor = transformation_factor

        self._container_in = Container(item_in)
        self._pipe = Pipe()
        self._pipe.pressure_in = 0
        self._container_out = Container(item_out)
        connect(self._container_in, self._pipe)
        connect(self._pipe, self._container_out)

        self.max_pressure_in = max_pressure_in
        self.max_pressure_out = max_pressure_out

        self.last_pipe_speed = None

        self.total_processed = 0

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self._container_in.value += (ts - self.last_ts) * self.speed('in')
        self.total_processed += (ts - self.last_ts) * self.speed('in')
        self._container_in.value -= (ts - self.last_ts) * self._pipe.current_speed
        self._container_out.value += (ts - self.last_ts) * self._pipe.current_speed * self.transformation_factor
        self._container_out.value -= (ts - self.last_ts) * self.speed('out')

        # close pressure asap
        if self.pipe('in') and self.total_processed == self.processing_limit:
            self.pipe('in').pressure_out = 0

    def update_pressure(self, ts):
        if self.pipe('in') and self.total_processed != self.processing_limit:
            self.pipe('in').pressure_out = self.max_pressure_in

        if self.pipe('out'):
            self.pipe('out').pressure_in = self.max_pressure_out

    def update_speed(self, ts):
        if self.processing_time == 0:
            # set new inner pressure at once
            self._pipe.pressure_in = self.speed('in')
        else:
            # set inner pressure delayed with processing time
            if self.last_pipe_speed != self.speed('in'):
                self.add_event('processing_container.set_pressure', ts + self.processing_time, {'pressure': self.speed('in')})
                self.last_pipe_speed = self.speed('in')

        self._pipe.update_speed(ts)

        if self.pipe('out') and abs(self._container_out.value) < ERROR:
            self.pipe('out').pressure_in = calc_minimum_pressure([self.pipe('out').pressure_in, self._pipe.current_speed])

    def on_set_pressure(self, topic, ts, event):
        self._pipe.pressure_in = event['pressure']

    def update_triggers(self, ts):
        speed_drain = self._pipe.current_speed - self.speed('out')
        if self._container_out.value > ERROR and speed_drain < -ERROR:
            # trigger when current value is finished with current speed
            eta = self._container_out.value / abs(speed_drain)
            self.add_event('update.trigger.empty_container', ts + eta, {})

        if self.processing_limit and self.speed('in') > ERROR:
            # trigger when processing limit is filled
            value_left = self.processing_limit - self.total_processed
            eta = value_left / self.speed('in')
            self.add_event('update.trigger.filled_limit', ts + eta, {})

    def __str__(self):
        return f'Processing Container: {self.id}'

    def stats(self):
        return {'container_in': self._container_in.stats(), 'pipe': self._pipe.stats(), 'container_out': self._container_out.stats()}

    def subscribe(self):
        self.event_manager.subscribe('processing_container.set_pressure', self.on_set_pressure)
