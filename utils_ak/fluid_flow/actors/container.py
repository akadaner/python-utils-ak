from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
from utils_ak.fluid_flow.calculations import ERROR
from utils_ak.fluid_flow.pressure import calc_minimum_pressure
import pandas as pd


class Container(Actor, PipeMixin):
    def __init__(self, id=None, item='default', max_pressures=None, limits=None):
        super().__init__(id)
        self.item = item
        self.value = 0

        self.gates_df = pd.DataFrame(index=['in', 'out'], columns=['max_pressure', 'limit', 'collected'])
        self.gates_df['max_pressure'] = max_pressures
        self.gates_df['max_pressure'] = self.gates_df.astype(float)
        self.gates_df['limits'] = limits
        self.gates_df['collected'] = 0

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self.value += (ts - self.last_ts) * self.speed('in')
        self.gates_df.at['in', 'collected'] += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')
        self.gates_df.at['out', 'collected'] += (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        if self.pipe('in'):
            self.pipe('in').pressures['out'] = self.gates_df.at['in', 'max_pressure']

        if self.pipe('out'):
            self.pipe('out').pressures['in'] = self.gates_df.at['out', 'max_pressure']

    def update_speed(self, ts):
        input_speed = self.speed('in')

        # set factual pressure for output
        if self.pipe('out') and abs(self.value) < ERROR:
            self.pipe('out').pressures['in'] = calc_minimum_pressure([self.pipe('out').pressures['in'], input_speed])

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        if self.value > ERROR and self.speed('drain') < -ERROR:
            eta = self.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Container {self.id} with {self.item}'

    def stats(self):
        return {'value': self.value}
