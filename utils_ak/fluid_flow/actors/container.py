from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
from utils_ak.fluid_flow.calculations import *
import pandas as pd


class Container(Actor, PipeMixin):
    def __init__(self, id, value=0, item='default', max_pressures=None, limits=None):
        super().__init__(id)
        self.item = item
        self.value = value

        self.df = pd.DataFrame(index=['in', 'out'], columns=['max_pressure', 'limit', 'collected'])
        self.df['max_pressure'] = max_pressures
        self.df['limit'] = limits
        self.df['collected'] = 0

        self.transactions = []

    def is_limit_reached(self, orient):
        if self.df.at[orient, 'limit'] and self.df.at[orient, 'collected'] == self.df.at[orient, 'limit']:
            return True
        return False

    def update_value(self, ts, factor=1):
        if self.last_ts is None:
            return

        self.add_value(ts, 'in', (ts - self.last_ts) * self.speed('in') * factor)
        self.add_value(ts, 'out', -(ts - self.last_ts) * self.speed('out'))

    def add_value(self, ts, orient, value):
        if not value:
            return
        self.value += value
        self.df.at[orient, 'collected'] += abs(value)
        self.transactions.append([self.last_ts, ts, value])

    def active_period(self):
        if not self.transactions:
            return
        return self.transactions[0][0], self.transactions[-1][1]

    def update_pressure(self, ts, orients=('in', 'out')):
        for orient in orients:
            if self.pipe(orient):
                pressure = self.df.at[orient, 'max_pressure'] if not self.is_limit_reached(orient) else 0
                self.pipe(orient).pressures[orient] = pressure

    def update_speed(self, ts, set_out_pressure=True):
        input_speed = self.speed('in')

        if set_out_pressure:
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
        return f'Container {self.id}:{self.item}'

    def stats(self):
        return {'value': self.value}

    def display_stats(self):
        return self.value
