from utils_ak.fluid_flow.actor import Actor
import numpy as np


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
