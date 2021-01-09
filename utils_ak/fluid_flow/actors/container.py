from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
from utils_ak.fluid_flow.calculations import ERROR
from utils_ak.fluid_flow.pressure import calc_minimum_pressure


class Container(Actor, PipeMixin):
    def __init__(self, id=None, item='default', max_pressure_in=None, max_pressure_out=None):
        super().__init__(id)
        self.item = item
        self.value = 0
        self.max_pressure_in = max_pressure_in
        self.max_pressure_out = max_pressure_out

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self.value += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        if self.pipe('in'):
            self.pipe('in').pressure_out = self.max_pressure_in

        if self.pipe('out'):
            self.pipe('out').pressure_in = self.max_pressure_out

    def update_speed(self, ts):
        input_speed = self.speed('in')

        # set factual pressure for output
        if self.pipe('out') and abs(self.value) < ERROR:
            self.pipe('out').pressure_in = calc_minimum_pressure([self.pipe('out').pressure_in, input_speed])

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        if self.value > ERROR and self.speed('drain') < -ERROR:
            eta = self.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Container {self.id} with {self.item}'

    def stats(self):
        return {'value': self.value}
