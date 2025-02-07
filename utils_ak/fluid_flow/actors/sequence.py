from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.pipe import pipe_switch, Piped, pipe_connect
from utils_ak.fluid_flow.fluid_flow import FluidFlow
from utils_ak.iteration import SimpleIterator

from functools import wraps


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        # - Switch the pipes to the inner containers temporarily

        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")

        # - Run function

        res = f(self, *args, **kwargs)

        # - Switch the pipes back

        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")

        # - Return decorated function result

        return res

    return inner


class Sequence(Actor, Piped):
    """A chain of actors piped together"""

    def __init__(self, name: str, containers: list[Actor]):
        # - Init

        super().__init__(name)
        assert len(containers) >= 2
        self.containers = containers

        # - Set IO containers

        self.io_containers = {"in": containers[0], "out": containers[-1]}

        # - Create the chain

        self.nodes = [self.containers[0]]
        for c1, c2 in SimpleIterator(self.containers).iter_sequences(2):
            pipe = pipe_connect(c1, c2)
            self.nodes.append(pipe)
            self.nodes.append(c2)

    # - Private methods

    def is_limit_reached(self, orient):
        return self.io_containers[orient].is_limit_reached(orient)

    # - Generic overrides

    def inner_actors(self):
        return self.nodes

    def __str__(self):
        return f"Sequence ({self.name})"

    def stats(self):
        return {node.name: node.stats() for node in self.nodes}

    def display_stats(self):
        return [node.display_stats() for node in self.containers]

    def active_periods(self, orient="in"):
        return self.io_containers[orient].active_periods(orient=orient)

    def state_snapshot(self):
        return [node.state_snapshot() for node in self.nodes]

    # - Updaters

    @switch
    def update_values(self, ts):
        for node in self.containers:
            node.update_values(ts)

    @switch
    def update_pressure(self, ts):
        for node in self.containers:
            node.update_pressure(ts)

    @switch
    def update_speed(self, ts):
        for node in self.nodes:
            node.update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in self.containers:
            node.update_triggers(ts)


def test():
    # - Test 1

    c1 = Container("Input", value=100, max_pressures=[None, 10])

    sequence = Sequence(
        "Sequence",
        containers=[Container(str(i), max_pressures=[5, None]) for i in range(3)],
    )

    c2 = Container("Ouput", max_pressures=[None, None])

    pipe_connect(c1, sequence)
    pipe_connect(sequence, c2)

    assert FluidFlow(c1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Sequence (Sequence) -> Container (Ouput)
""",
            "str(flow)": """\
Flow:
    Sequence (Sequence): [0.0, 0.0, 0.0]
    Container (Ouput): 100.0\
""",
            "nodes": {
                "Container (Input)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 10.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 20.0, -100.0]]",
                },
                "Sequence (Sequence)": [
                    {
                        "value": 0.0,
                        "df": "[{'index': 'in', 'max_pressure': 5.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 100.0}]",
                        "transactions": "[[0, 20.0, 100.0], [0, 20.0, -100.0]]",
                    },
                    {},
                    {
                        "value": 0.0,
                        "df": "[{'index': 'in', 'max_pressure': 5.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 100.0}]",
                        "transactions": "[[0, 20.0, 100.0], [0, 20.0, -100.0]]",
                    },
                    {},
                    {
                        "value": 0.0,
                        "df": "[{'index': 'in', 'max_pressure': 5.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 100.0}]",
                        "transactions": "[[0, 20.0, 100.0], [0, 20.0, -100.0]]",
                    },
                ],
                "Container (Ouput)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 20.0, 100.0]]",
                },
            },
        }
    )

    # - Test 2

    c1 = Container("Input", value=100, max_pressures=[None, 10])

    sequence = Sequence(
        "Sequence",
        containers=[
            Container("0", max_pressures=[2, None]),
            Container("1", max_pressures=[1, None]),  # slow one
        ],
    )

    c2 = Container("Output", max_pressures=[None, None])

    pipe_connect(c1, sequence)
    pipe_connect(sequence, c2)

    assert FluidFlow(c1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Sequence (Sequence) -> Container (Output)
""",
            "str(flow)": """\
Flow:
    Sequence (Sequence): [0.0, 0.0]
    Container (Output): 100.0\
""",
            "nodes": {
                "Container (Input)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 10.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 50.0, -100.0]]",
                },
                "Sequence (Sequence)": [
                    {
                        "value": 0.0,
                        "df": "[{'index': 'in', 'max_pressure': 2.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 100.0}]",
                        "transactions": "[[0, 50.0, 100.0], [0, 50.0, -50.0], [50.0, 100.0, -50.0]]",
                    },
                    {},
                    {
                        "value": 0.0,
                        "df": "[{'index': 'in', 'max_pressure': 1.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 100.0}]",
                        "transactions": "[[0, 50.0, 50.0], [0, 50.0, -50.0], [50.0, 100.0, 50.0], [50.0, 100.0, -50.0]]",
                    },
                ],
                "Container (Output)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 50.0, 50.0], [50.0, 100.0, 50.0]]",
                },
            },
        }
    )


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
