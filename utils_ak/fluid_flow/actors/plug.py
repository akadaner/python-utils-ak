from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import Piped


class Plug(Actor, Piped):
    """No input and output, just a plug."""

    def __init__(self, name=None):
        super().__init__(name)

    def update_pressure(self, ts):
        """No input and output"""
        for pipe in self.parents:
            pipe.pressures["in"] = 0
        for pipe in self.children:
            pipe.pressures["out"] = 0

    def __str__(self):
        return f"Plug ({self.name})"
