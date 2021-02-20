from utils_ak.dag import *

from utils_ak.clock import *
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import *
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.calculations import *

from functools import wraps


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")

        res = f(self, *args, **kwargs)

        pipe_switch(self, self.io_containers["in"], "in")
        pipe_switch(self, self.io_containers["out"], "out")
        return res

    return inner


class Processor(Actor, PipeMixin):
    def __init__(
        self,
        name,
        items=None,
        processing_time=0,
        transformation_factor=1,
        max_pressures=None,
        limits=None,
    ):
        super().__init__(name)

        max_pressures = max_pressures or [None, None]
        limits = limits or [None, None]
        items = items or ["default", "default"]
        self.io_containers = {
            "in": Container(
                "In",
                item=items[0],
                max_pressures=(max_pressures[0], None),
                limits=(limits[0], None),
            ),
            "out": Container(
                "Out",
                item=items[1],
                max_pressures=(None, max_pressures[1]),
                limits=(None, limits[1]),
            ),
        }
        self._pipe = pipe_connect(
            self.io_containers["in"], self.io_containers["out"], "_pipe"
        )
        self.processing_time = processing_time

        self.last_pipe_speed = None
        self.transformation_factor = transformation_factor

    def is_limit_reached(self, orient):
        return self.io_containers[orient].is_limit_reached(orient)

    def inner_actors(self):
        return [self.io_containers["in"], self._pipe, self.io_containers["out"]]

    def on_set_pressure(self, topic, ts, event):
        self._pipe.set_pressure("out", event["pressure"], event["item"])

    def subscribe(self):
        self.event_manager.subscribe(
            f"processor.set_pressure.{self.id}", self.on_set_pressure
        )

    @switch
    def update_value(self, ts):
        self.io_containers["in"].update_value(ts)
        self.io_containers["out"].update_value(ts, factor=self.transformation_factor)

    @switch
    def update_pressure(self, ts):
        self.io_containers["in"].update_pressure(ts, orients=["in"])
        self.io_containers["out"].update_pressure(ts, orients=["out"])

    @switch
    def update_speed(self, ts):
        self.io_containers["in"].update_speed(ts, set_out_pressure=False)

        if self.processing_time == 0:
            # set new inner pressure at once
            self._pipe.set_pressure(
                "out",
                self.io_containers["in"].speed("in"),
                self.io_containers["out"].item,
            )
        else:
            # set inner pressure delayed with processing time
            if self.last_pipe_speed != self.io_containers["in"].speed("in"):
                self.add_event(
                    f"processor.set_pressure.{self.id}",
                    ts + self.processing_time,
                    {
                        "pressure": self.io_containers["in"].speed("in"),
                        "item": self.io_containers["out"].item,
                    },
                )
                self.last_pipe_speed = self.io_containers["in"].speed("in")

        self._pipe.update_speed(ts)

        self.io_containers["out"].update_speed(ts)

    @switch
    def update_triggers(self, ts):
        for node in [self.io_containers["in"], self.io_containers["out"]]:
            node.update_triggers(ts)

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
