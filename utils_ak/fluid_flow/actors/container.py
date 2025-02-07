from typing import Optional

from utils_ak.clock import *
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import PipeMixin
from utils_ak.fluid_flow.calculations import *
import pandas as pd


class Container(Actor, PipeMixin):
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

    # - Updaters

    def update_value(self, ts, factor=1):
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
        for orient in orients:
            if self.pipe(orient):
                pressure = (
                    self.df.at[orient, "max_pressure"]
                    if not (
                        limit_reached := self.df.at[orient, "limit"]
                        and abs(self.df.at[orient, "collected"] - self.df.at[orient, "limit"]) < ERROR
                    )
                    else 0
                )
                self.pipe(orient).set_pressure(orient, pressure, self.item)

    def update_speed(self, ts, set_out_pressure=True):
        input_speed = self.speed("in")

        if set_out_pressure:
            if self.pipe("out") and abs(self.value) < ERROR:
                self.pipe("out").set_pressure(
                    "out",
                    nanmin([self.pipe("out").pressures["out"], input_speed]),
                    self.item,
                )

    def update_triggers(self, ts):
        values = []
        if self.drain() < 0:
            values.append(["empty_container", self.value, self.drain()])

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

    # - Test 1

    container1 = Container("Input", value=100, max_pressures=[None, 50])
    container2 = Container("Output")

    pipe_connect(container1, container2)

    flow = FluidFlow(container1)
    run_fluid_flow(flow)

    # # - Test 2
    #
    # container1 = Container("Input", value=100, max_pressures=[None, 50], limits=[None, 30])
    # container2 = Container("Output")
    #
    # pipe_connect(container1, container2)
    #
    # flow = FluidFlow(container1)
    # run_fluid_flow(flow)
    #
    # # - Test 3
    #
    # container1 = Container("Input", value=100, max_pressures=[None, 50], limits=[None, 30])
    # container2 = Container("Output", max_pressures=[5, None], limits=[20, None])
    #
    # pipe_connect(container1, container2)
    #
    # flow = FluidFlow(container1)
    # run_fluid_flow(flow)


if __name__ == "__main__":
    test()
