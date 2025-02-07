from typing import Optional

from inline_snapshot import snapshot
from numpy import nan

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.calculations import ERROR, nanmin
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import Piped, pipe_connect
import pandas as pd

from utils_ak.fluid_flow.fluid_flow import FluidFlow


class Container(Actor, Piped):
    """A container receives and outputs value with input and output pipes.

    Pipes define pressures. Difference between them is the speed of the flow.
    """

    def __init__(
        self,
        name: str,
        value: float = 0,
        item: str = "default",
        max_pressures: list[Optional[float]] = [None, None],
        limits: list[Optional[float]] = [None, None],
    ):
        # - Init

        super().__init__(name)
        self.item = item

        # - Init value

        self.value = value

        # - Init dataframe

        self.df = pd.DataFrame(index=["in", "out"], columns=["max_pressure", "limit", "collected"])
        self.df["max_pressure"] = max_pressures
        self.df["limit"] = limits
        self.df["collected"] = 0.0

        # - Init transactions

        self.transactions = []

    # - General overridable

    def reset(self):
        super().reset()
        self.transactions = []

    def active_periods(self, orient="in"):
        if not self.transactions:
            return []
        return [[self.item, self.transactions[0][0], self.transactions[-1][1]]]

    def __str__(self):
        return f"Container ({self.name}{':' + self.item if self.item != 'default' else ''})"

    def stats(self):
        return {"value": self.value}

    def display_stats(self):
        return self.value

    def state_snapshot(self):
        return {
            "value": self.value,
            "df": str(self.df.reset_index().to_dict(orient="records")),
            "transactions": str(self.transactions),
        }

    # - Private methods

    def is_limit_reached(self, orient):
        return (
            self.df.at[orient, "limit"] and abs(self.df.at[orient, "collected"] - self.df.at[orient, "limit"]) < ERROR
        )

    # - Updaters

    def update_values(self, ts, input_factor=1):
        """Something flowed in and out"""
        if self.last_ts is None:
            return

        def add_value(ts, orient, value):
            if not value:
                return
            self.value += value
            self.df.at[orient, "collected"] += abs(value)
            self.transactions.append([self.last_ts, ts, value])

        add_value(ts, "in", (ts - self.last_ts) * self.speed("in") * input_factor)
        add_value(ts, "out", -(ts - self.last_ts) * self.speed("out"))

    def update_pressure(self, ts, orients=("in", "out")):
        """Disable pressure if limit is specified and limit is reached"""
        for orient in orients:
            if self.pipe(orient):
                self.pipe(orient).set_pressure(
                    orient=orient,
                    pressure=(self.df.at[orient, "max_pressure"] if not self.is_limit_reached(orient) else 0),
                    item=self.item,
                )

    def update_speed(self, ts, set_out_pressure=True):
        """Set out pressure if there is a pipe out to minimum of input speed and out pressure"""
        input_speed = self.speed("in")

        if set_out_pressure:
            if self.pipe("out") and abs(self.value) < ERROR:
                self.pipe("out").set_pressure(
                    "out",
                    nanmin([self.pipe("out").pressures["out"], input_speed]),
                    self.item,
                )

    def update_triggers(self, ts):
        """Add events for empty container and limits"""
        values = []
        if self.excess_speed() < 0:
            values.append(["empty_container", self.value, self.excess_speed()])

        for orient in ["in", "out"]:
            if self.df.at[orient, "limit"]:
                values.append(
                    [
                        f"{orient} limit",
                        self.df.at[orient, "limit"] - self.df.at[orient, "collected"],
                        self.speed(orient),
                    ]
                )

        values = [value for value in values if value[1] > ERROR and abs(value[2]) > ERROR]
        ETAs = [value[1] / abs(value[2]) for value in values]
        if ETAs:
            self.add_event("update.trigger", ts + min(ETAs), {})


def test():
    # - Configure loguru

    from utils_ak.loguru import configure_loguru

    configure_loguru()

    # - Test 1

    container1 = Container("Input", value=100, max_pressures=[None, 50])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    assert FluidFlow(container1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe (1) -> Container (Output) -> Pipe (2) -> Stub (Top) -> [None]
""",
            "str(flow)": """\
Flow:
    Container (Output): 100.0\
""",
            "nodes": {
                "Container (Input)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': None, 'collected': 100.0}]",
                    "transactions": "[[0, 2.0, -100.0]]",
                },
                "Container (Output)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 2.0, 100.0]]",
                },
            },
        }
    )

    # - Test 2

    container1 = Container("Input", value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    assert FluidFlow(container1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> Pipe (3) -> Container (Output) -> Pipe (4) -> Stub (Top) -> [None]
""",
            "str(flow)": """\
Flow:
    Container (Input): 70.0
    Container (Output): 30.0\
""",
            "nodes": {
                "Container (Input)": {
                    "value": 70.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': 30.0, 'collected': 30.0}]",
                    "transactions": "[[0, 0.6, -30.0]]",
                },
                "Container (Output)": {
                    "value": 30.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 30.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 0.6, 30.0]]",
                },
            },
        }
    )

    # - Test 3

    container1 = Container("C1", value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container("C2", max_pressures=[5, None], limits=[20, None])
    container3 = Container("C3", max_pressures=[None, None], limits=[None, None])

    pipe_connect(container1, container2)
    pipe_connect(container2, container3)

    assert FluidFlow(container1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (C1) -> Pipe (5) -> Container (C2) -> Pipe (6) -> Container (C3) -> Pipe (7) -> Stub (Top) -> [None]
""",
            "str(flow)": """\
Flow:
    Container (C1): 80.0
    Container (C3): 20.0\
""",
            "nodes": {
                "Container (C1)": {
                    "value": 80.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': 30.0, 'collected': 20.0}]",
                    "transactions": "[[0, 4.0, -20.0]]",
                },
                "Container (C2)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': 5.0, 'limit': 20.0, 'collected': 20.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 20.0}]",
                    "transactions": "[[0, 4.0, 20.0], [0, 4.0, -20.0]]",
                },
                "Container (C3)": {
                    "value": 20.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 20.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 4.0, 20.0]]",
                },
            },
        }
    )


if __name__ == "__main__":
    # test()
    run_inline_snapshot_tests(mode="update_all")
