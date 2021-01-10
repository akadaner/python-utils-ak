from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *
from utils_ak.iteration import SimpleBoundedIterator

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


class Queue(Actor, PipeMixin):
    def __init__(self, id, containers):
        super().__init__(id)
        self.containers = containers
        self.iterators = {'in': SimpleBoundedIterator(containers, 0), 'out': SimpleBoundedIterator(containers, 0)}

    def inner_actors(self):
        return self.containers

    def update_value_orient(self, ts, orient='in'):
        pass

    def update_value(self, ts):
        pass

    def update_pressure(self, ts):
        pass

    def update_speed(self, ts):
        pass

    def update_triggers(self, ts):
        pass

    def __str__(self):
        return f'Queue: {self.id}'

    def stats(self):
        return {[(node.id, node.stats()) for node in self.containers]}
