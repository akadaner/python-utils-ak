from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.container import Container

from functools import wraps

from utils_ak.fluid_flow.actors.pipe import pipe_switch, PipeMixin, pipe_connect
from utils_ak.fluid_flow.fluid_flow import FluidFlow


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        # - Switch the pipes to the inner containers temporarily

        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")

        # - Run function

        res = f(self, *args, **kwargs)

        #  - Switch the pipes back
        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")

        # - Return decorated function result

        return res

    return inner


class Processor(Actor, PipeMixin):
    """Receives value in an input container and converts its to the output container."""

    def __init__(
        self,
        name: str,
        items: list[str] = ["default", "default"],
        lag: float = 0,
        transformation_factor: float = 1,
        max_pressures: list[float] = [None, None],
        limits: list[float] = [None, None],
    ):
        # - Init

        super().__init__(name)
        self.lag = lag
        self.transformation_factor = transformation_factor

        # - Define IO containers

        self.io_containers = {
            "in": Container(
                "In",
                item=items[0],
                max_pressures=[max_pressures[0], None],
                limits=[limits[0], None],
            ),
            "out": Container(
                "Out",
                item=items[1],
                max_pressures=[None, max_pressures[1]],
                limits=[None, limits[1]],
            ),
        }
        self._pipe = pipe_connect(self.io_containers["in"], self.io_containers["out"], "_pipe")
        self.last_pipe_speed = None

    # - Private methods

    def is_limit_reached(self, orient):
        return self.io_containers[orient].is_limit_reached(orient)

    def on_set_pressure(self, topic, ts, event):
        self._pipe.set_pressure("out", event["pressure"], event["item"])

    # - Generic overrides

    def inner_actors(self):
        return [self.io_containers["in"], self._pipe, self.io_containers["out"]]

    def subscribe(self):
        self.event_manager.subscribe(f"processor.set_pressure.{self.id}", self.on_set_pressure)

    def __str__(self):
        return f"Processor: {self.name}"

    def stats(self):
        return {
            node.name: node.stats()
            for node in [
                self.io_containers["in"],
                self._pipe,
                self.io_containers["out"],
            ]
        }

    def display_stats(self):
        return [
            self.io_containers["in"].display_stats(),
            self.io_containers["out"].display_stats(),
        ]

    def active_periods(self, orient="in"):
        return self.io_containers[orient].active_periods()

    # - Updaters

    @switch
    def update_values(self, ts):
        self.io_containers["in"].update_values(ts)
        self.io_containers["out"].update_values(ts, input_factor=self.transformation_factor)

    @switch
    def update_pressure(self, ts):
        self.io_containers["in"].update_pressure(ts, orients=["in"])
        self.io_containers["out"].update_pressure(ts, orients=["out"])

    @switch
    def update_speed(self, ts):
        # - Update speed of the input container

        self.io_containers["in"].update_speed(ts, set_out_pressure=False)

        # - Update speeds

        if self.lag == 0:
            # set new inner pressure at once
            self._pipe.set_pressure(
                "out",
                self.io_containers["in"].speed("in"),
                self.io_containers["out"].item,
            )
        else:
            # set inner pressure delayed with the lag
            if self.last_pipe_speed != self.io_containers["in"].speed("in"):
                self.add_event(
                    f"processor.set_pressure.{self.id}",
                    ts + self.lag,
                    {
                        "pressure": self.io_containers["in"].speed("in"),
                        "item": self.io_containers["out"].item,
                    },
                )
                self.last_pipe_speed = self.io_containers["in"].speed("in")

        # - Update pipe speed

        self._pipe.update_speed(ts)

        self.io_containers["out"].update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in [self.io_containers["in"], self.io_containers["out"]]:
            node.update_triggers(ts)


def test():
    # - Test 1

    container = Container("Input", value=100, max_pressures=[None, 10])
    processor = Processor("Output")
    pipe_connect(container, processor)
    assert FluidFlow(container).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe 1 -> Processor: Output -> Pipe 2 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Processor: Output: [0.0, 100.0]\
""",
            "nodes": {
                "Input": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 10.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 10.0, -100.0]]",
                },
                "1": {},
                "Output": {},
                "2": {},
                "Top": {},
            },
        }
    )

    # - Test 2

    container = Container("Input", value=100)
    processor = Processor("Output", lag=5, max_pressures=[10, None], transformation_factor=2.0)
    pipe_connect(container, processor)
    assert FluidFlow(container).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe 3 -> Processor: Output -> Pipe 4 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Processor: Output: [0.0, 100.0]\
""",
            "nodes": {
                "Input": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 5, -50.0], [5, 10.0, -50.0]]",
                },
                "3": {},
                "Output": {},
                "4": {},
                "Top": {},
            },
        }
    )

    # - Test 3: zero pressure

    container = Container("Input", value=100)
    processor = Processor("Output", max_pressures=[0, 0])
    pipe_connect(container, processor)
    assert FluidFlow(container).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe 5 -> Processor: Output -> Pipe 6 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Container (Input): 100
    Processor: Output: [0, 0]\
""",
            "nodes": {
                "Input": {
                    "value": 100,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[]",
                },
                "5": {},
                "Output": {},
                "6": {},
                "Top": {},
            },
        }
    )

    # - Test 4: limit

    container = Container("Input", value=100, max_pressures=[None, 10])
    processor = Processor("Output", limits=[50, None])
    pipe_connect(container, processor)
    assert FluidFlow(container).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe 7 -> Processor: Output -> Pipe 8 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Container (Input): 50.0
    Processor: Output: [0.0, 50.0]\
""",
            "nodes": {
                "Input": {
                    "value": 50.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 10.0, 'limit': None, 'collected': 50.0}]",
                    "transactions": "[[0, 5.0, -50.0]]",
                },
                "7": {},
                "Output": {},
                "8": {},
                "Top": {},
            },
        }
    )


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
