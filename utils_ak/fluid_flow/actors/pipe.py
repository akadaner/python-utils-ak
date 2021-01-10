from utils_ak.dag import connect
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.calculations import *

def cast_pipe(pipe_obj):
    if isinstance(pipe_obj, str):
        return Pipe(pipe_obj)
    elif isinstance(pipe_obj, Pipe):
        return pipe_obj
    else:
        raise Exception('Unknown pipe object')


class Pipe(Actor):
    def __init__(self, id=None):
        super().__init__(id)
        self.current_speed = 0
        self.pressures = {'out': None, 'in': None}

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
        self.current_speed = nanmin(self.pressures.values())

    def __str__(self):
        return f'Pipe {self.id}'

    def stats(self):
        return {'current_speed': self.current_speed, 'pressures': self.pressures}


class PipeMixin:
    def pipe(self, orient):
        if orient == 'in':
            nodes = self.parents
        elif orient == 'out':
            nodes = self.children

        if not nodes:
            return
        else:
            assert len(nodes) == 1
            return nodes[0]

    def speed(self, orient):
        if orient == 'in':
            if not self.pipe('in'):
                return 0
            return self.pipe('in').current_speed
        elif orient == 'out':
            if not self.pipe('out'):
                return 0
            return self.pipe('out').current_speed

    def drain(self):
        return self.speed('in') - self.speed('out')

def pipe_together(node1, node2, pipe='Pipe'):
    pipe = cast_pipe(pipe)
    connect(node1, pipe)
    connect(pipe, node2)
