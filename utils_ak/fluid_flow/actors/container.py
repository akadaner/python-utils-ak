from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
from utils_ak.fluid_flow.calculations import *
import pandas as pd


class Container(Actor, PipeMixin):
    def __init__(self, id=None, item='default', max_pressures=None, limits=None):
        super().__init__(id)
        self.item = item
        self.value = 0

        self.df = pd.DataFrame(index=['in', 'out'], columns=['max_pressure', 'limit', 'collected'])
        self.df['max_pressure'] = max_pressures
        self.df['limit'] = limits
        self.df['collected'] = 0

    def is_limit_reached(self, orient):
        if self.df.at[orient, 'limit'] and self.df.at[orient, 'collected'] == self.df.at[orient, 'limit']:
            return True
        return False

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self.value += (ts - self.last_ts) * self.speed('in')
        self.df.at['in', 'collected'] += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')
        self.df.at['out', 'collected'] += (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        for orient in ['in', 'out']:
            if self.pipe(orient):
                pressure = self.df.at[orient, 'max_pressure'] if not self.is_limit_reached(orient) else 0
                self.pipe(orient).pressures[orient] = pressure

    def update_speed(self, ts):
        input_speed = self.speed('in')

        # set factual pressure for output
        if self.pipe('out') and abs(self.value) < ERROR:
            self.pipe('out').pressures['out'] = nanmin([self.pipe('out').pressures['out'], input_speed])

    def update_triggers(self, ts):
        values = []
        if self.drain() < 0:
            values.append(['empty_container', self.value, self.drain()])

        for orient in ['in', 'out']:
            if self.df.at[orient, 'limit']:
                values.append([f'{orient} limit', self.df.at[orient, 'limit'] - self.df.at[orient, 'collected'], self.speed(orient)])

        df = pd.DataFrame(values, columns=['name', 'left', 'speed'])
        df = df[df['left'] > ERROR]
        df = df[df['speed'].abs() > ERROR]
        df['eta'] = df['left'] / df['speed'].abs()
        df = df.sort_values(by='eta')

        if len(df) > 0:
            self.add_event('update.trigger', ts + df.iloc[0]['eta'], {})

    def __str__(self):
        return f'Container {self.id} with {self.item}'

    def stats(self):
        return {'value': self.value}
