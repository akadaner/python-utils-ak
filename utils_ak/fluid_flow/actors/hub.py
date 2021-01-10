from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin, Pipe
from utils_ak.fluid_flow.calculations import *


class Hub(Actor):
    def __init__(self, id):
        super().__init__(id)

    def update_pressure(self, ts):
        # todo: hardcode. Need updated children pressures. Better solution?
        for pipe in self.children:
            pipe.child.update_pressure(ts)

        if any(pipe.pressures['in'] is None for pipe in self.children):
            for pipe in self.parents:
                pipe.pressures['in'] = None
        else:
            total_output_pressure = sum(pipe.pressures['in'] for pipe in self.children)
            left = total_output_pressure

            for pipe in self.parents:
                pipe.pressures['in'] = nanmin([left, pipe.pressures['out']])
                left -= nanmin([left, pipe.pressures['out']])

    def update_speed(self, ts):
        assert all(isinstance(pipe, Pipe) for pipe in self.parents + self.children)

        total_input_speed = sum(pipe.current_speed for pipe in self.parents)
        left = total_input_speed

        for pipe in self.children:
            pipe.pressures['out'] = nanmin([left, pipe.pressures['in']])
            left -= nanmin([left, pipe.pressures['in']])

    def __str__(self):
        return f'Hub: {self.id}'
