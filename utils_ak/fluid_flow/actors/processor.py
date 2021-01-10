from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *

from functools import wraps


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        pipe_switch(self, self.containers['in'], 'in')
        pipe_switch(self, self.containers['out'], 'out')

        res = f(self, *args, **kwargs)

        pipe_switch(self, self.containers['in'], 'in')
        pipe_switch(self, self.containers['out'], 'out')
        return res
    return inner


class Processor(Actor, PipeMixin):
    def __init__(self, id, items=None, processing_time=0, transformation_factor=1, max_pressures=None, limits=None):
        super().__init__(id)

        max_pressures = max_pressures or [None, None]
        limits = limits or [None, None]
        items = items or ['default', 'default']
        self.containers = {'in': Container('In', item=items[0], max_pressures=(max_pressures[0], None), limits=(limits[0], None)),
                           'out': Container('Out', item=items[1], max_pressures=(None, max_pressures[1]), limits=(None, limits[1]))}
        self._pipe = pipe_together(self.containers['in'], self.containers['out'], '_pipe')
        self.processing_time = processing_time

        self.last_pipe_speed = None
        self.transformation_factor = transformation_factor

    def is_limit_reached(self, orient):
        return self.containers[orient].is_limit_reached(orient)

    def inner_actors(self):
        return [self.containers['in'], self._pipe, self.containers['out']]

    def on_set_pressure(self, topic, ts, event):
        self._pipe.pressures['out'] = event['pressure']

    def subscribe(self):
        self.event_manager.subscribe(f'processor.set_pressure.{self.id}', self.on_set_pressure)

    @switch
    def update_value(self, ts):
        self.containers['in'].update_value(ts)
        self.containers['out'].update_value(ts, factor=self.transformation_factor)

    @switch
    def update_pressure(self, ts):
        self.containers['in'].update_pressure(ts, orients=['in'])
        self.containers['out'].update_pressure(ts, orients=['out'])

    @switch
    def update_speed(self, ts):
        self.containers['in'].update_speed(ts, set_out_pressure=False)

        if self.processing_time == 0:
            # set new inner pressure at once
            self._pipe.pressures['out'] = self.containers['in'].speed('in')
        else:
            # set inner pressure delayed with processing time
            if self.last_pipe_speed != self.containers['in'].speed('in'):
                self.add_event(f'processor.set_pressure.{self.id}', ts + self.processing_time, {'pressure': self.containers['in'].speed('in')})
                self.last_pipe_speed = self.containers['in'].speed('in')

        self._pipe.update_speed(ts)

        self.containers['out'].update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in [self.containers['in'], self.containers['out']]:
            node.update_triggers(ts)

    def __str__(self):
        return f'Processor: {self.id}'

    def stats(self):
        return {node.id: node.stats() for node in [self.containers['in'], self._pipe, self.containers['out']]}

    def display_stats(self):
        return [self.containers['in'].display_stats(), self.containers['out'].display_stats()]