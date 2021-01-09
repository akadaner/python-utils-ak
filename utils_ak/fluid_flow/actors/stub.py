from utils_ak.fluid_flow.actor import Actor


class Stub(Actor):
    def __init__(self, id=None):
        super().__init__(id)

    def update_pressure(self, ts):
        for pipe in self.parents:
            pipe.pressure_out = 0
        for pipe in self.children:
            pipe.pressure_in = 0

    def __str__(self):
        return f'Stub {self.id}'