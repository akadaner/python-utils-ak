from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.pipe import Piped, Pipe, pipe_connect
from utils_ak.fluid_flow.calculations import nanmin
from utils_ak.fluid_flow.fluid_flow import FluidFlow


class Hub(Actor):
    """Redirects input flows to the output flows for the corresponding items"""

    def __init__(self, name: str):
        super().__init__(name)

    def update_pressure(self, ts):
        # - Update pressure for downstream actors

        # todo: hardcode. Need updated children pressures. Better solution? Update pressure top-down?
        for pipe in self.children:
            pipe.child.update_pressure(ts)

        # - Get input item

        parent_items = [pipe.current_item for pipe in self.parents]
        assert len(set(parent_items)) == 1
        item = parent_items[0]

        # - Get children with the input item

        children_pipes_with_item = [pipe for pipe in self.children if pipe.current_item == item]

        # - Update parent pressures

        if any(child_pipe.pressures["in"] is None for child_pipe in children_pipes_with_item):
            # if ANYONE is not receiving input, stop all the flow of the parents
            for pipe in self.parents:
                pipe.pressures["in"] = None
        else:
            # - Sum input pressure

            total_output_pressure = sum(pipe.pressures["in"] for pipe in children_pipes_with_item)

            # - Distribute pressure to the parents

            left = total_output_pressure

            for pipe in self.parents:
                pipe.pressures["in"] = nanmin([left, pipe.pressures["out"]])
                left -= nanmin([left, pipe.pressures["out"]])

    def update_speed(self, ts):
        """NOTE: actually updates children pressures. Forgot why I did this in the first place"""

        # - Assert all children and parents are pipes

        assert all(isinstance(pipe, Pipe) for pipe in self.parents + self.children)

        # - Get input item

        parent_items = [pipe.current_item for pipe in self.parents]
        assert len(set(parent_items)) == 1
        item = parent_items[0]

        # - Get children with the input item

        children_pipes_with_item = [pipe for pipe in self.children if pipe.current_item == item]

        # - Calculate total parents speed

        total_input_speed = sum(pipe.current_speed for pipe in self.parents)

        # - Iterate over the children and set according pressures

        left = total_input_speed

        for pipe in self.children:
            if pipe in children_pipes_with_item:
                pipe.pressures["out"] = nanmin([left, pipe.pressures["in"]])
                left -= nanmin([left, pipe.pressures["in"]])
            else:
                pipe.pressures["out"] = 0

    def __str__(self):
        return f"Hub ({self.name})"


def test():
    # - Test 1

    parent = Container("Parent", value=100, max_pressures=[None, 20])

    hub = Hub("Hub")

    child1 = Container("Child1", max_pressures=[15, None])
    child2 = Container("Child2", max_pressures=[10, None])  # only 5 will be left for child2

    pipe_connect(parent, hub)
    pipe_connect(hub, child1)
    pipe_connect(hub, child2)

    assert FluidFlow(parent).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Parent) -> Hub (Hub) -> [Pipe (2), Pipe (3)]
Pipe (2) -> Container (Child1) -> [Plug (Bottom)]
Pipe (3) -> Container (Child2) -> [Plug (Bottom)]
""",
            "str(flow)": """\
Flow:
    Container (Child1): 75.0
    Container (Child2): 25.0\
""",
            "nodes": {
                "Container (Parent)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 5.0, -100.0]]",
                },
                "Container (Child1)": {
                    "value": 75.0,
                    "df": "[{'index': 'in', 'max_pressure': 15.0, 'limit': None, 'collected': 75.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 5.0, 75.0]]",
                },
                "Container (Child2)": {
                    "value": 25.0,
                    "df": "[{'index': 'in', 'max_pressure': 10.0, 'limit': None, 'collected': 25.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 5.0, 25.0]]",
                },
            },
        }
    )

    # - Test 2: limits

    parent = Container("Parent", value=100, max_pressures=[None, 20])

    hub = Hub("Hub")

    child1 = Container("Child1", max_pressures=[15, None], limits=[30, None])
    child2 = Container("Child2", max_pressures=[10, None])  # will get 5 at first, then 10 after 2 seconds

    pipe_connect(parent, hub)
    pipe_connect(hub, child1)
    pipe_connect(hub, child2)

    assert FluidFlow(parent).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Parent) -> Hub (Hub) -> [Pipe (7), Pipe (8)]
Pipe (7) -> Container (Child1) -> [Plug (Bottom)]
Pipe (8) -> Container (Child2) -> [Plug (Bottom)]
""",
            "str(flow)": """\
Flow:
    Container (Child1): 30.0
    Container (Child2): 70.0\
""",
            "nodes": {
                "Container (Parent)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 2.0, -40.0], [2.0, 5.0, -30.0], [5.0, 8.0, -30.0]]",
                },
                "Container (Child1)": {
                    "value": 30.0,
                    "df": "[{'index': 'in', 'max_pressure': 15.0, 'limit': 30.0, 'collected': 30.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}]",
                    "transactions": "[[0, 2.0, 30.0]]",
                },
                "Container (Child2)": {
                    "value": 70.0,
                    "df": "[{'index': 'in', 'max_pressure': 10.0, 'limit': None, 'collected': 70.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 2.0, 10.0], [2.0, 5.0, 30.0], [5.0, 8.0, 30.0]]",
                },
            },
        }
    )

    # - Test 3: different items

    parent = Container("Parent", item="a", value=100, max_pressures=[None, 20])

    hub = Hub("Hub")

    child1 = Container("Child1", item="b", max_pressures=[15, None])
    child2 = Container("Child2", item="a", max_pressures=[10, None])

    pipe_connect(parent, hub)
    pipe_connect(hub, child1)
    pipe_connect(hub, child2)

    assert FluidFlow(parent).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Parent:a) -> Hub (Hub) -> [Pipe (12), Pipe (13)]
Pipe (12) -> Container (Child1:b) -> [Plug (Bottom)]
Pipe (13) -> Container (Child2:a) -> [Plug (Bottom)]
""",
            "str(flow)": """\
Flow:
    Container (Child2:a): 100.0\
""",
            "nodes": {
                "Container (Parent:a)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 10.0, -100.0]]",
                },
                "Container (Child1:b)": {
                    "value": 0,
                    "df": "[{'index': 'in', 'max_pressure': 15.0, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[]",
                },
                "Container (Child2:a)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': 10.0, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 10.0, 100.0]]",
                },
            },
        }
    )


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
