from utils_ak.dag import *
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.calculations import *


def cast_pipe(pipe_obj):
    if isinstance(pipe_obj, str):
        return Pipe(pipe_obj)
    elif isinstance(pipe_obj, Pipe):
        return pipe_obj
    else:
        raise Exception("Unknown pipe object")


class Pipe(Actor):
    """Connects one actor to another."""

    def __init__(self, name=None):
        super().__init__(name)
        self.current_speed = 0
        self.current_item = None
        self.pressures = {"out": None, "in": None}

    @property
    def parent(self):
        if not self.parents:
            return
        assert len(self.parents) == 1
        return self.parents[0]

    @property
    def child(self):
        if not self.children:
            return
        assert len(self.children) == 1
        return self.children[0]

    def update_speed(self, ts):
        self.current_speed = nanmin(self.pressures.values(), require_any=False)

        if np.isnan(self.current_speed):
            self.current_speed = 0

    def __str__(self):
        return f"Pipe ({self.name})"

    def stats(self):
        return {"current_speed": self.current_speed, "pressures": self.pressures}

    def set_pressure(self, orient, pressure, item):
        self.pressures[orient] = pressure
        self.current_item = item


class PipeMixin:
    def pipe(self, orient):
        if orient == "in":
            nodes = self.parents
        elif orient == "out":
            nodes = self.children

        if not nodes:
            return
        else:
            assert len(nodes) == 1
            return nodes[0]

    def speed(self, orient):
        if orient == "in":
            if not self.pipe("in"):
                return 0
            return self.pipe("in").current_speed
        elif orient == "out":
            if not self.pipe("out"):
                return 0
            return self.pipe("out").current_speed

    def excess_speed(self):
        """Excess flow"""
        return self.speed("in") - self.speed("out")


def pipe_connect(node1, node2, pipe=None):
    if not pipe:
        # todo later: hardcode, make properly. This way it's easier to navigate between them all
        setattr(pipe_connect, "counter", getattr(pipe_connect, "counter", 0) + 1)
        pipe = cast_pipe(str(pipe_connect.counter))
    else:
        pipe = cast_pipe(pipe)
    connect(node1, pipe)
    connect(pipe, node2)
    return pipe


def pipe_disconnect(node1, node2):
    # find commmon pipe
    input_pipes = [node for node in node1.children if isinstance(node, Pipe)]
    output_pipes = [node for node in node2.parents if isinstance(node, Pipe)]

    intersection = set(input_pipes) & set(output_pipes)

    assert len(intersection) > 0
    for pipe in intersection:
        disconnect(node1, pipe)
        disconnect(pipe, node2)


def pipe_switch(node1, node2, orient="in"):
    if node1 == node2:
        return
    piped_nodes = [node for node in [node1, node2] if node.pipe(orient)]

    if not piped_nodes:
        return

    pipe1, pipe2 = node1.pipe(orient), node2.pipe(orient)

    if orient == "in":
        disconnect(pipe1, node1)
        disconnect(pipe2, node2)
        connect(pipe1, node2)
        connect(pipe2, node1)
    elif orient == "out":
        disconnect(node1, pipe1)
        disconnect(node2, pipe2)
        connect(node2, pipe1)
        connect(node1, pipe2)


def test():
    # - Test pipe switch

    from utils_ak.fluid_flow.actors.container import Container

    ci1 = Container("I1")
    ci2 = Container("I2")
    co1 = Container("O1")
    co2 = Container("O2")

    pipe_connect(ci1, co1, "1")
    pipe_connect(ci2, co2, "2")
    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 1 -> Container (O1) -> [None]
""")
    assert ci2.schema() == snapshot("""\
Container (I2) -> Pipe 2 -> Container (O2) -> [None]
""")

    pipe_switch(co1, co2, "in")
    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 1 -> Container (O2) -> [None]
""")
    assert ci2.schema() == snapshot("""\
Container (I2) -> Pipe 2 -> Container (O1) -> [None]
""")

    pipe_switch(co1, co2, "in")
    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 1 -> Container (O1) -> [None]
""")
    assert ci2.schema() == snapshot("""\
Container (I2) -> Pipe 2 -> Container (O2) -> [None]
""")

    pipe_switch(ci1, ci2, "out")
    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 2 -> Container (O2) -> [None]
""")
    assert ci2.schema() == snapshot("""\
Container (I2) -> Pipe 1 -> Container (O1) -> [None]
""")

    # - Test pipe switch 2

    ci1 = Container("I1")
    co1 = Container("O1")
    co2 = Container("O2")

    pipe_connect(ci1, co1)

    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 1 -> Container (O1) -> [None]
""")

    pipe_switch(co1, co2, "in")

    assert ci1.schema() == snapshot("""\
Container (I1) -> Pipe 1 -> Container (O2) -> [None]
""")


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
