from typing import Callable

from inline_snapshot import snapshot

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.pipe import pipe_switch, pipe_connect, Piped
from utils_ak.fluid_flow.actors.plug import Plug
from functools import wraps

from utils_ak.fluid_flow.fluid_flow import FluidFlow


def switch(f):
    @wraps(f)
    def inner(self, *args, **kwargs):
        # - Switch the pipes to the inner containers temporarily

        pipe_switch(self, self.current_actor, "in")
        pipe_switch(self, self.current_actor, "out")

        # - Run function

        res = f(self, *args, **kwargs)

        #  - Switch the pipes back
        pipe_switch(self, self.current_actor, "in")
        pipe_switch(self, self.current_actor, "out")

        # - Return decorated function result

        return res

    return inner


class TumbleredActor(Actor, Piped):
    def __init__(
        self,
        name: str,
        actor: Actor,
        tumbler_func: Callable,  # if returns None, tumbler is disabled. Be careful, if you don't stop the tumbler, the event manager will run forever
        working_on_start: bool = False,
    ):
        super().__init__(name)

        # - Arguments

        self.actor = actor
        self.tumbler_func = tumbler_func

        # - State

        self.plug = Plug(f"{name}_plug")
        self.working = working_on_start
        self.current_actor = actor if working_on_start else self.plug
        self.next_toggle_time = None

    # - Generic overrides

    def inner_actors(self):
        return [self.actor, self.plug]

    def __str__(self):
        return f"TumbleredActor ({self.name})"

    def state_snapshot(self):
        return {str(self.actor) : self.actor.state_snapshot()}

    # - Updaters

    def update_values(self, ts):
        """Apply state changes when event occurs, then update values."""

        # - Switch tumbler on/off if needed

        if self.next_toggle_time and ts >= self.next_toggle_time:
            self.working = not self.working
            self.current_actor = self.actor if self.working else self.plug
            self.next_toggle_time = None

        # - Try to get the next toggle time or stop the tumbler

        if not self.next_toggle_time and self.tumbler_func:
            self.next_toggle_time = self.tumbler_func(ts)

            if self.next_toggle_time is None:
                # stopping the tumbler
                self.tumbler_func = None
            else:
                self.add_event("tumbler.toggle", self.next_toggle_time, {})

        # - Switch the pipes to the current actor temporarily

        pipe_switch(self, self.current_actor, "in")
        pipe_switch(self, self.current_actor, "out")

        # - Update values

        self.current_actor.update_values(ts)

        # - Switch the pipes back

        pipe_switch(self, self.current_actor, "in")
        pipe_switch(self, self.current_actor, "out")

    @switch
    def update_pressure(self, ts):
        self.current_actor.update_pressure(ts)

    @switch
    def update_speed(self, ts):
        self.current_actor.update_speed(ts)

    @switch
    def update_triggers(self, ts):
        self.current_actor.update_triggers(ts)


def test():
    # - Test 1

    container1 = Container(
        "Input",
        value=3,
        max_pressures=[None, 1],
    )
    container2 = TumbleredActor(
        "Tumbler",
        actor=Container("Output"),
        tumbler_func=lambda ts: ts + 1 if ts < 10 else None,
        working_on_start=False,
    )
    pipe_connect(container1, container2)

    assert FluidFlow(container1).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Input) -> TumbleredActor (Tumbler)
""",
            "str(self)": "Flow:",
            "nodes": {
                "Container (Input)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 1.0, 'limit': None, 'collected': 3.0}]",
                    "transactions": "[[1, 2, -1.0], [3, 4.0, -1.0], [5.0, 6.0, -1.0]]",
                }
            },
        }
    )


if __name__ == "__main__":
    from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests

    run_inline_snapshot_tests(mode="update_all")
