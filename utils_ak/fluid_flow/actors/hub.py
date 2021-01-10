from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin, Pipe
from utils_ak.fluid_flow.calculations import *


class Hub(Actor):
    def __init__(self, id):
        super().__init__(id)

    def update_pressure(self, ts):
        if any(pipe.pressure_out is None for pipe in self.children):
            for pipe in self.parents:
                pipe.pressure_out = None
        else:
            total_output_pressure = sum(pipe.pressure_out for pipe in self.children)
            left = total_output_pressure

            for pipe in self.parents:
                pipe.pressure_out = nanmin([left, pipe.pressure_in])
                left -= nanmin([left, pipe.pressure_in])

    def update_speed(self, ts):
        assert all(isinstance(pipe, Pipe) for pipe in self.parents + self.children)

        total_input_speed = sum(pipe.current_speed for pipe in self.parents)
        left = total_input_speed

        for pipe in self.children:
            pipe.pressure_in = nanmin([left, pipe.pressure_out])
            left -= nanmin([left, pipe.pressure_out])

    def __str__(self):
        return f'Hub: {self.id}'