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
        pipe_switch(self, self.current('in'), 'in')
        pipe_switch(self, self.current('out'), 'out')

        res = f(self, *args, **kwargs)

        pipe_switch(self, self.current('in'), 'in')
        pipe_switch(self, self.current('out'), 'out')
        return res
    return inner


class Queue(Actor, PipeMixin):
    def __init__(self, id, containers):
        super().__init__(id)
        self.containers = containers
        self.iterators = {'in': SimpleBoundedIterator(containers, 0), 'out': SimpleBoundedIterator(containers, 0)}

    def current(self, orient):
        return self.iterators[orient].current()

    def inner_actors(self):
        return self.containers

    @switch
    def update_value(self, ts):
        self.current('in').update_value(ts)
        if self.current('in') != self.current('out'):
            self.current('out').update_value(ts)

        for orient in ['in', 'out']:
            if self.current(orient).is_limit_reached(orient):
                # try to go to the next queue element
                old = self.current(orient)
                new = self.iterators[orient].next()
                pipe_switch(old, new, orient)

    @switch
    def update_pressure(self, ts):
        for node in self.inner_actors():
            node.update_pressure(ts)

    @switch
    def update_speed(self, ts):
        for node in self.inner_actors():
            node.update_speed(ts)
    @switch
    def update_triggers(self, ts):
        for node in self.inner_actors():
            node.update_triggers(ts)

    def __str__(self):
        return f'Queue: {self.id}'

    def stats(self):
        return [[node.id, node.stats()] for node in self.containers]

    def display_stats(self):
        return [(node.id, node.display_stats()) for node in self.containers]