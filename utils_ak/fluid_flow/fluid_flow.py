from utils_ak.coder import cast_js
from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors.pipe import pipe_connect, Pipe
from utils_ak.fluid_flow.actors.plug import Plug
from utils_ak.simple_event_manager import SimpleEventManager

from loguru import logger

"""
Fluid flow runs a DAG of nodes (Actors) and updates them in a sequential manner:
- Update value
- Update pressure
- Update speed
- Update triggers
- Update last_ts

Each of the methods is defined within the Actor. 


"""


class FluidFlow:
    def __init__(self, root: Actor, verbose: bool = False):
        # - Set attributes

        self.root = root
        self.verbose = verbose

        # - Create a single top node or each leaf

        bottom = Plug("Bottom")
        for i, leaf in enumerate(self.root.leaves()):
            pipe_connect(leaf, bottom)

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
            "update_values",
            "update_pressure",
            "update_speed",
            "update_triggers",
            "update_last_ts",
        ]:
            for node in self.root.iterate("down"):
                getattr(node, method, lambda ts: None)(ts)

    def state_snapshot(self):
        result = {
            "schema": self.root.schema(skip_rule=lambda node: isinstance(node, Pipe) or isinstance(node, Plug)),
            "str(flow)": str(self),
            "nodes": {},
        }

        for node in self.root.iterate("down"):
            if node.state_snapshot():
                result["nodes"][str(node)] = node.state_snapshot()

        return result

    def run(self):
        # - Init event manager

        event_manager = SimpleEventManager()

        # - Subscribe to all nodes of the flow

        for node in self.root.iterate("down"):
            node.set_event_manager(event_manager)
            node.subscribe()

        # - Subscribe to all events for flow.update

        event_manager.subscribe("", self.update)

        # - Add start event

        event_manager.add_event("update", 0, {})

        # - Run event manager

        event_manager.run()

        # - Return self

        return self
