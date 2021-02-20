from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin, Pipe
from utils_ak.fluid_flow.calculations import *


class Hub(Actor):
    def __init__(self, name):
        super().__init__(name)

    def update_pressure(self, ts):
        # todo: hardcode. Need updated children pressures. Better solution? Update pressure top-down?
        for pipe in self.children:
            pipe.child.update_pressure(ts)

        parent_items = [pipe.current_item for pipe in self.parents]
        assert len(set(parent_items)) == 1
        item = parent_items[0]
        children_with_item = [
            pipe for pipe in self.children if pipe.current_item == item
        ]

        if any(pipe.pressures["in"] is None for pipe in children_with_item):
            for pipe in self.parents:
                pipe.pressures["in"] = None
        else:
            total_output_pressure = sum(
                pipe.pressures["in"] for pipe in children_with_item
            )
            left = total_output_pressure

            for pipe in self.parents:
                pipe.pressures["in"] = nanmin([left, pipe.pressures["out"]])
                left -= nanmin([left, pipe.pressures["out"]])

    def update_speed(self, ts):
        assert all(isinstance(pipe, Pipe) for pipe in self.parents + self.children)

        parent_items = [pipe.current_item for pipe in self.parents]
        assert len(set(parent_items)) == 1
        item = parent_items[0]
        children_with_item = [
            pipe for pipe in self.children if pipe.current_item == item
        ]

        total_input_speed = sum(pipe.current_speed for pipe in self.parents)
        left = total_input_speed

        for pipe in self.children:
            if pipe in children_with_item:
                pipe.pressures["out"] = nanmin([left, pipe.pressures["in"]])
                left -= nanmin([left, pipe.pressures["in"]])
            else:
                pipe.pressures["out"] = 0

    def __str__(self):
        return f"Hub: {self.name}"
