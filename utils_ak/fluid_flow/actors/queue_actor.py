"""NOTE: renamed this file since some modules stop working if there is a file named queue.py in the same folder"""

import pandas as pd
from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.actor import Actor

from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.pipe import pipe_switch, PipeMixin, pipe_connect
from utils_ak.fluid_flow.fluid_flow import FluidFlow, run_fluid_flow
from utils_ak.iteration import SimpleIterator

from functools import wraps


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        # - Switch the pipes to the inner lines temporarily

        pipe_switch(self, self.current("in"), "in")
        pipe_switch(self, self.current("out"), "out")

        # - Run function

        res = f(self, *args, **kwargs)

        # - Switch the pipes back

        pipe_switch(self, self.current("in"), "in")
        pipe_switch(self, self.current("out"), "out")

        # - Return decorated function result

        return res

    return inner


class Queue(Actor, PipeMixin):
    """Queue receives or outputs values sequentially."""

    def __init__(
        self,
        name: str,
        lines: list[Actor],
        break_funcs_by_orient: dict = {},  # {"in": lambda old, new: float = 0, "out": lambda old, new: float = 0}
    ):
        # - Arguments

        super().__init__(name)
        self.lines = lines

        # - State dataframe

        self.df = pd.DataFrame(index=["in", "out"], columns=["iterator", "break_func", "paused"])
        self.df["iterator"] = [
            SimpleIterator(lines, 0) for _ in range(2)
        ]  # current line that's receiving or giving the value
        self.df["break_func"] = self.default_break_func
        self.df["paused"] = False

        for orient, break_func in break_funcs_by_orient.items():
            self.df.at[orient, "break_func"] = break_func

        # - Init breaks

        self.breaks = []  # ts_beg, ts_end

    # - Private methods

    def default_break_func(self, old: Actor, new: Actor) -> float:
        return 0

    def current(self, orient):
        return self.df.at[orient, "iterator"].current()

    # - Generic overrides

    def inner_actors(self):
        return self.lines

    def __str__(self):
        return f"Queue: {self.name}"

    def stats(self):
        return [[node.name, node.stats()] for node in self.lines]

    def display_stats(self):
        return [(node.name, node.display_stats()) for node in self.lines]

    def active_periods(self, orient="in"):
        return sum([node.active_periods(orient) for node in self.lines], [])

    @switch
    def _on_resume(self, topic, ts, event):
        self.df.at[event["orient"], "paused"] = False
        old = self.current(event["orient"])
        new = self.df.at[event["orient"], "iterator"].next(return_out_strategy="last", update_index=True)
        pipe_switch(old, new, event["orient"])

    def subscribe(self):
        super().subscribe()
        self.event_manager.subscribe(f"queue.resume.{self.id}", self._on_resume)

    def reset(self):
        super().reset()
        self.breaks = []

    def state_snapshot(self):
        return {
            "queue": [line.state_snapshot() for line in self.lines],
            "breaks": self.breaks,
        }

    # - Updaters

    @switch
    def update_values(self, ts):
        # - Update value for children

        for line in self.lines:
            line.update_values(ts)

        # - Process reaching limits for current lines

        for orient in ["in", "out"]:
            if self.current(orient).is_limit_reached(orient):
                # - If already paused, just leave it be

                if self.df.at[orient, "paused"]:
                    continue

                # - Get old and new line

                old_line = self.current(orient)
                new_line = self.df.at[orient, "iterator"].next(return_out_strategy="last", update_index=False)

                # - Get break period

                break_period = self.df.at[orient, "break_func"](old_line, new_line)

                # - Process switch

                if old_line != new_line:
                    if break_period:
                        # - Add break event, pause

                        self.breaks.append([ts, ts + break_period])
                        self.df.at[orient, "paused"] = True
                        self.add_event(
                            f"queue.resume.{self.id}",
                            ts + break_period,
                            {"orient": orient},
                        )
                    else:
                        # - Switch the pipe to the new line

                        pipe_switch(old_line, new_line, orient)

                        # - Update iterator

                        self.df.at[orient, "iterator"].next(return_out_strategy="last", update_index=True)

    @switch
    def update_pressure(self, ts):
        for node in self.inner_actors():
            node.update_pressure(ts)

    @switch
    def update_speed(self, ts):
        for node in self.inner_actors():
            node.update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in self.inner_actors():
            node.update_triggers(ts)


def test():
    # - Test 1: in

    parent = Container("Parent", value=100, max_pressures=[None, 20])

    pipe_connect(
        parent,
        Queue(
            "Queue",
            [
                Container("Child1", max_pressures=[20, None], limits=[40, None]),
                Container("Child2", max_pressures=[10, None], limits=[50, None]),
            ],
        ),
        "parent-queue",
    )

    flow = FluidFlow(parent)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Container (Parent) -> Pipe parent-queue -> Queue: Queue -> Pipe 1 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Container (Parent): 10.0
    Queue: Queue: [["Child1", 40.0], ["Child2", 50.0]]\
""",
            "nodes": {
                "Parent": {
                    "value": 10.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': None, 'collected': 90.0}]",
                    "transactions": "[[0, 2.0, -40.0], [2.0, 5.0, -30.0], [5.0, 7.0, -20.0]]",
                },
                "parent-queue": {},
                "Queue": {
                    "queue": [
                        {
                            "value": 40.0,
                            "df": "[{'index': 'in', 'max_pressure': 20.0, 'limit': 40.0, 'collected': 40.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}]",
                            "transactions": "[[0, 2.0, 40.0]]",
                        },
                        {
                            "value": 50.0,
                            "df": "[{'index': 'in', 'max_pressure': 10.0, 'limit': 50.0, 'collected': 50.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}]",
                            "transactions": "[[2.0, 5.0, 30.0], [5.0, 7.0, 20.0]]",
                        },
                    ],
                    "breaks": [],
                },
                "1": {},
                "Top": {},
            },
        }
    )

    # - Test 2: out

    parent1 = Container("Parent1", value=100, max_pressures=[None, 10], limits=[None, 100])
    parent2 = Container("Parent2", value=100, max_pressures=[None, 20], limits=[None, 100])
    queue = Queue("Parent", [parent1, parent2])

    child = Container("Child", max_pressures=[None, None])
    pipe_connect(queue, child, "parent-queue")

    flow = FluidFlow(queue)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Queue: Parent -> Pipe parent-queue -> Container (Child) -> Pipe 2 -> Stub Top -> [None]
""",
            "str(flow)": """\
Flow:
    Queue: Parent: [["Parent1", 0.0], ["Parent2", 0.0]]
    Container (Child): 200.0\
""",
            "nodes": {
                "Parent": {
                    "queue": [
                        {
                            "value": 0.0,
                            "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 10.0, 'limit': 100.0, 'collected': 100.0}]",
                            "transactions": "[[0, 10.0, -100.0]]",
                        },
                        {
                            "value": 0.0,
                            "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': 100.0, 'collected': 100.0}]",
                            "transactions": "[[10.0, 15.0, -100.0]]",
                        },
                    ],
                    "breaks": [],
                },
                "parent-queue": {},
                "Child": {
                    "value": 200.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 200.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 10.0, 100.0], [10.0, 15.0, 100.0]]",
                },
                "2": {},
                "Top": {},
            },
        }
    )

    # # - Test 3
    #
    # parent = Container("Parent", value=100, max_pressures=[None, 20])
    #
    # child1 = Processor("Child1", max_pressures=[20, None], processing_time=5, limits=[40, None])
    # child2 = Processor("Child2", max_pressures=[10, None], processing_time=5, limits=[50, None])
    #
    # queue = Queue("Queue", [child1, child2])
    #
    # pipe_connect(parent, queue, "parent-queue")
    #
    # flow = FluidFlow(parent, verbose=True)
    # run_fluid_flow(flow)
    #
    # # - Test 4 with different items
    #
    # parent1 = Container("Parent1", item="a", value=100, max_pressures=[None, 10], limits=[None, 100])
    # parent2 = Container("Parent2", item="b", value=100, max_pressures=[None, 20], limits=[None, 100])
    # queue = Queue("Parent", [parent1, parent2])
    #
    # hub = Hub("hub")
    # child1 = Container("Child1", item="a", max_pressures=[None, None])
    # child2 = Container("Child2", item="b", max_pressures=[None, None])
    #
    # pipe_connect(queue, hub, "parent-hub")
    # pipe_connect(hub, child1, "hub-child1")
    # pipe_connect(hub, child2, "hub-child2")
    # flow = FluidFlow(queue, verbose=True)
    # run_fluid_flow(flow)


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
