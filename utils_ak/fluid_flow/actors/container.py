from typing import Optional

from inline_snapshot import snapshot
from numpy import nan

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.calculations import ERROR, nanmin
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
import pandas as pd


class Container(Actor, PipeMixin):
    """A container can have ONE input and ONE output. Container is connected to them via one pipe per each."""

    def __init__(
        self,
        name: str,
        value: float = 0,
        item: str = "default",
        max_pressures: Optional[list[float]] = None,
        limits: Optional[list[float]] = None,
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
        return f"Container {self.name}:{self.item}"

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

    # - Updaters

    def update_value(self, ts, factor=1):
        """Something flowed in and out"""
        if self.last_ts is None:
            return

        def add_value(ts, orient, value):
            if not value:
                return
            self.value += value
            self.df.at[orient, "collected"] += abs(value)
            self.transactions.append([self.last_ts, ts, value])

        add_value(ts, "in", (ts - self.last_ts) * self.speed("in") * factor)
        add_value(ts, "out", -(ts - self.last_ts) * self.speed("out"))

    def update_pressure(self, ts, orients=("in", "out")):
        """Disable pressure if limit is specified and limit is reached"""
        for orient in orients:
            if self.pipe(orient):
                self.pipe(orient).set_pressure(
                    orient=orient,
                    pressure=(
                        self.df.at[orient, "max_pressure"]
                        if not (
                            limit_reached := self.df.at[orient, "limit"]
                            and abs(self.df.at[orient, "collected"] - self.df.at[orient, "limit"]) < ERROR
                        )
                        else 0
                    ),
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
    from utils_ak.fluid_flow import Container, pipe_connect, FluidFlow, run_fluid_flow

    # - Configure loguru

    from utils_ak.loguru import configure_loguru

    configure_loguru()

    # - Test 1

    container1 = Container("Input", value=100, max_pressures=[None, 50])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    flow = FluidFlow(container1)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Flow:
    Container Output:default: 100.0\
""",
            "Input": {
                "value": 0.0,
                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': None, 'collected': 100.0}]",
                "transactions": "[[0, 2.0, -100.0]]",
            },
            "Container Input:default -> Container Output:default": {},
            "Output": {
                "value": 100.0,
                "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                "transactions": "[[0, 2.0, 100.0]]",
            },
            "Top parent 0": {},
            "Top": {},
        }
    )

    # - Test 1+

    container1 = Container("Input", value=100, max_pressures=[None, 50])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    flow = FluidFlow(container1)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Flow:
    Container Output:default: 100.0\
""",
            "Input": {
                "value": 0.0,
                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': None, 'collected': 100.0}]",
                "transactions": "[[0, 2.0, -100.0]]",
            },
            "Container Input:default -> Container Output:default": {},
            "Output": {
                "value": 100.0,
                "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                "transactions": "[[0, 2.0, 100.0]]",
            },
            "Top parent 0": {},
            "Top": {},
        }
    )

    # - Test 2

    container1 = Container("Input", value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    flow = FluidFlow(container1)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Flow:
    Container Input:default: 70.0
    Container Output:default: 30.0\
""",
            "Input": {
                "value": 70.0,
                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': 30.0, 'collected': 30.0}]",
                "transactions": "[[0, 0.6, -30.0]]",
            },
            "Container Input:default -> Container Output:default": {},
            "Output": {
                "value": 30.0,
                "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 30.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                "transactions": "[[0, 0.6, 30.0]]",
            },
            "Top parent 0": {},
            "Top": {},
        }
    )

    # - Test 3

    container1 = Container("Input", value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container("Output", max_pressures=[5, None], limits=[20, None])

    pipe_connect(container1, container2)

    flow = FluidFlow(container1)
    run_fluid_flow(flow)

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Flow:
    Container Input:default: 80.0
    Container Output:default: 20.0\
""",
            "Input": {
                "value": 80.0,
                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}, {'index': 'out', 'max_pressure': 50.0, 'limit': 30.0, 'collected': 20.0}]",
                "transactions": "[[0, 4.0, -20.0]]",
            },
            "Container Input:default -> Container Output:default": {},
            "Output": {
                "value": 20.0,
                "df": "[{'index': 'in', 'max_pressure': 5.0, 'limit': 20.0, 'collected': 20.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 0.0}]",
                "transactions": "[[0, 4.0, 20.0]]",
            },
            "Top parent 0": {},
            "Top": {},
        }
    )


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
