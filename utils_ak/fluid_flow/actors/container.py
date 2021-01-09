from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.cable import CableMixin
from utils_ak.fluid_flow.calculations import ERROR

class Container(Actor, CableMixin):
    def __init__(self, id=None, max_pressure_out=None):
        super().__init__(id)
        self.value = 0
        self.max_pressure_out = max_pressure_out

    def update_value(self, ts):
        if self.last_ts is None:
            return
        self.value += (ts - self.last_ts) * self.speed('in')
        self.value -= (ts - self.last_ts) * self.speed('out')

    def update_pressure(self, ts):
        if self.cable('out'):
            input_speed = self.speed('in')

            if abs(self.value) < ERROR:
                self.cable('out').pressure_in = min(self.max_pressure_out, input_speed)
            else:
                self.cable('out').pressure_in = self.max_pressure_out

    def update_triggers(self, ts):
        # trigger when current value is finished with current speed
        if self.value > ERROR and self.speed('drain') < -ERROR:
            eta = self.value / abs(self.speed('drain'))
            self.add_event('update.trigger', ts + eta, {})

    def __str__(self):
        return f'Container {self.id}'

    def stats(self):
        return {'value': self.value}
