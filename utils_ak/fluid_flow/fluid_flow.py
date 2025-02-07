from utils_ak.coder import cast_js
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import pipe_connect
from utils_ak.fluid_flow.actors.stub import Stub
from utils_ak.simple_event_manager import SimpleEventManager

from loguru import logger

"""
Fluid flow runs a DAG of nodes (Actors) and updates them in a sequential manner:
- Update value
- Update pressure
- Update speed
- Update triggers
- Update last_ts

Each of the methods is defined within the Actor

"""


class FluidFlow:
    def __init__(self, root: Actor, verbose: bool = False):
        # - Set attributes

        self.root = root
        self.verbose = verbose

        # - Create a single top node or each leaf

        top = Stub("Top")
        for i, leaf in enumerate(self.root.leaves()):
            pipe_connect(leaf, top)

    def __str__(self):
        values = ["Flow:"]
        for node in self.root.iterate("down"):
            # values.append(' ' * 4 + str(node) + ': ' + cast_js(node.stats()))
            if node.display_stats():
                values.append(" " * 4 + str(node) + ": " + cast_js(node.display_stats()))
        return "\n".join(values)

    def __repr__(self):
        return str(self)

    def update(self, topic: str, ts: float, event: dict):
        """Iterate updates for all children nodes (BFS) sequentially for each method"""
        for method in [
            "update_value",
            "update_pressure",
            "update_speed",
            "update_triggers",
            "update_last_ts",
        ]:
            if self.verbose:
                logger.info(f"Updating {method}")
                print(self)

            for node in self.root.iterate("down"):
                getattr(node, method, lambda ts: None)(ts)

            if self.verbose:
                logger.info(f"Updated {method}")
                print(self)

    def state_snapshot(self):
        result = {"schema": self.root.schema(), "str(flow)": str(self), "nodes": {}}

        for node in self.root.iterate("down"):
            result["nodes"][node.name] = node.state_snapshot()

        return result


def run_fluid_flow(flow: FluidFlow):
    # - Init event manager

    event_manager = SimpleEventManager()

    # - Subscribe to all nodes of the flow

    for node in flow.root.iterate("down"):
        node.set_event_manager(event_manager)
        node.subscribe()

    # - Subscribe to all events for flow.update

    event_manager.subscribe("update", flow.update)

    # - Add start event

    event_manager.add_event("update", 0, {})

    # - Run event manager

    event_manager.run()
