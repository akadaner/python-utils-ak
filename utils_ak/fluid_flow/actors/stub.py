from utils_ak.fluid_flow.actor import Actor


class Stub(Actor):
    def __init__(self, id=None):
        super().__init__(id)

    def __str__(self):
        return f'Stub {self.id}'