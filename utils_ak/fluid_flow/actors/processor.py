from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *

from functools import wraps


def connector(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        if self.pipe('in'):
            pipe_in = self.pipe('in')
            disconnect(pipe_in, self)
            connect(pipe_in, self.containers['in'])
        if self.pipe('out'):
            pipe_out = self.pipe('out')
            disconnect(self, pipe_out)
            connect(self.containers['out'], pipe_out)

        res = f(self, *args, **kwargs)

        if self.containers['in'].pipe('in'):
            pipe_in = self.containers['in'].pipe('in')
            disconnect(pipe_in, self.containers['in'])
            connect(pipe_in, self)
        if self.containers['out'].pipe('out'):
            pipe_out = self.containers['out'].pipe('out')
            disconnect(self.containers['out'], pipe_out)
            connect(self, pipe_out)
        return res
    return inner


class Processor(Actor, PipeMixin):
    def __init__(self, id, containers, processing_time=0, transformation_factor=1):
        super().__init__(id)
        self.containers = containers
        self._pipe = pipe_together(containers['in'], containers['out'])
        self.processing_time = processing_time

        self.last_pipe_speed = None
        self.transformation_factor = transformation_factor

    def on_set_pressure(self, topic, ts, event):
        self._pipe.pressure_in = event['pressure']

    def subscribe(self):
        self.event_manager.subscribe('processing_container.set_pressure', self.on_set_pressure)

    @connector
    def update_value(self, ts):
        for node in [self.containers['in'], self.containers['out']]:
            node.update_value(ts)

    @connector
    def update_pressure(self, ts):
        for node in [self.containers['in'], self.containers['out']]:
            node.update_pressure(ts)

    @connector
    def update_speed(self, ts):
        self.containers['in'].update_speed(ts)
        if self.processing_time == 0:
            # set new inner pressure at once
            self._pipe.pressure_in = self.speed('in') * self.transformation_factor
        else:
            # set inner pressure delayed with processing time
            if self.last_pipe_speed != self.speed('in'):
                self.add_event('processing_container.set_pressure', ts + self.processing_time, {'pressure': self.speed('in') * self.transformation_factor})
                self.last_pipe_speed = self.speed('in') * self.transformation_factor
        self._pipe.update_speed(ts)

        self.containers['out'].update_speed(ts)

    @connector
    def update_triggers(self, ts):
        for node in [self.containers['in'], self.containers['out']]:
            node.update_triggers(ts)

    @connector
    def update_last_ts(self, ts):
        for node in [self.containers['in'], self._pipe, self.containers['out']]:
            node.update_last_ts(ts)

    def __str__(self):
        return f'Processing Container: {self.id}'

    def stats(self):
        return {node.id: node.stats() for node in [self.containers['in'], self._pipe, self.containers['out']]}
