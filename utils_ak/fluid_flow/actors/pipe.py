from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.pressure import calc_minimum_pressure
import numpy as np


class Pipe(Actor):
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

    @property
    def pressure(self, orient='in'):
        assert orient in ['in', 'out']
        if orient == 'in':
            return self.pressure_in
        elif orient == 'out':
            return self.pressure_out

    def update_speed(self, ts):
        self.current_speed = calc_minimum_pressure([self.pressure_in, self.pressure_out])

    def __str__(self):
        return f'Pipe {self.id}'

    def stats(self):
        return {'current_speed': self.current_speed, 'pressure_in': self.pressure_in, 'pressure_out': self.pressure_out}


class PipeMixin:
    def pipe(self, orient):
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
            if not self.pipe('in'):
                return 0
            return self.pipe('in').current_speed
        elif speed_type == 'out':
            if not self.pipe('out'):
                return 0
            return self.pipe('out').current_speed
        elif speed_type == 'drain':
            return self.speed('in') - self.speed('out')
