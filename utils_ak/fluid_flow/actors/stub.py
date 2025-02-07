from utils_ak.fluid_flow.actor import Actor


class Stub(Actor):
    """Does nothing. It's just a place for the pipes to connect."""

    def __init__(self, name=None):
        super().__init__(name)

    def __str__(self):
        return f"Stub ({self.name})"
