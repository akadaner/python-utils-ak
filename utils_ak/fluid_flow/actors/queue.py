import pandas as pd
from utils_ak.dag import *

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *
from utils_ak.iteration import SimpleIterator

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
    def __init__(self, id, containers, break_funcs=None):
        super().__init__(id)
        self.containers = containers

        self.df = pd.DataFrame(index=['in', 'out'], columns=['iterators', 'break_funcs', 'paused'])
        self.df['iterator'] = [SimpleIterator(containers, 0) for _ in range(2)]
        self.df['break_func'] = self.default_break_func
        self.df['paused'] = False

        break_funcs = break_funcs or {}
        for orient, break_func in break_funcs.items():
            self.df.at[orient, 'break_func'] = break_func

        self.breaks = [] # ts_beg, ts_end

    def default_break_func(self, old, new):
        return 0

    def current(self, orient):
        return self.df.at[orient, 'iterator'].current()

    def inner_actors(self):
        return self.containers

    @switch
    def update_value(self, ts):
        self.current('in').update_value(ts)
        if self.current('in') != self.current('out'):
            self.current('out').update_value(ts)

        for orient in ['in', 'out']:
            if self.current(orient).is_limit_reached(orient):
                if self.df.at[orient, 'paused']:
                    continue
                old = self.current(orient)
                new = self.df.at[orient, 'iterator'].next(return_last_if_out=True, update_index=False)
                break_period = self.df.at[orient, 'break_func'](old, new)

                if old != new:
                    if break_period:
                        self.breaks.append([ts, ts + break_period])
                        self.df.at[orient, 'paused'] = True
                        self.add_event(f'queue.resume.{self.id}', ts + break_period, {'orient': orient})
                    else:
                        pipe_switch(old, new, orient)
                        self.df.at[orient, 'iterator'].next(return_last_if_out=True, update_index=True)

            # todo: del
            # print('Current', self.id, orient, self.current(orient), self.current(orient).is_limit_reached(orient), self.current(orient).containers['out'].df)

    @switch
    def on_resume(self, topic, ts, event):
        self.df.at[event['orient'], 'paused'] = False
        old = self.current(event['orient'])
        new = self.df.at[event['orient'], 'iterator'].next(return_last_if_out=True)
        pipe_switch(old, new, event['orient'])

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

    def active_periods(self, orient='in'):
        return sum([node.active_periods(orient) for node in self.containers], [])

    def subscribe(self):
        super().subscribe()
        self.event_manager.subscribe(f'queue.resume.{self.id}', self.on_resume)

    def reset(self):
        super().reset()
        self.breaks = []