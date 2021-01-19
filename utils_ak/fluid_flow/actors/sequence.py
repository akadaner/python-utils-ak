from utils_ak.dag import *

from utils_ak.clock import *
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *
from utils_ak.iteration import SimpleIterator

from functools import wraps


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        pipe_switch(self, self.io_containers['in'], 'in')
        pipe_switch(self, self.io_containers['out'], 'out')

        res = f(self, *args, **kwargs)

        pipe_switch(self, self.io_containers['in'], 'in')
        pipe_switch(self, self.io_containers['out'], 'out')
        return res
    return inner


class Sequence(Actor, PipeMixin):
    def __init__(self, id, containers):
        super().__init__(id)
        assert len(containers) >= 2
        self.containers = containers
        self.io_containers = {'in': containers[0], 'out': containers[-1]}

        self.nodes = [self.containers[0]]
        for c1, c2 in SimpleIterator(self.containers).iter_sequences(2):
            pipe = pipe_connect(c1, c2)
            self.nodes.append(pipe)
            self.nodes.append(c2)

    def is_limit_reached(self, orient):
        return self.io_containers[orient].is_limit_reached(orient)

    def inner_actors(self):
        return self.nodes

    @switch
    def update_value(self, ts):
        for node in self.containers:
            node.update_value(ts)

    @switch
    def update_pressure(self, ts):
        for node in self.containers:
            node.update_pressure(ts)

    @switch
    def update_speed(self, ts):
        for node in self.nodes:
            node.update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in self.containers:
            node.update_triggers(ts)

    def __str__(self):
        return f'Sequence: {self.id}'

    def stats(self):
        return {node.id: node.stats() for node in self.nodes}

    def display_stats(self):
        return [node.display_stats() for node in self.containers]

    def active_periods(self, orient='in'):
        return self.io_containers[orient].active_periods(orient=orient)
