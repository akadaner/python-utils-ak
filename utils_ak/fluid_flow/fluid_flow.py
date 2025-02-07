from utils_ak.coder import cast_js
from utils_ak.dag import DAGNode
from utils_ak.simple_event_manager import SimpleEventManager

from utils_ak.fluid_flow.actors import pipe_connect, Stub
from loguru import logger


class FluidFlow:
    def __init__(self, root: DAGNode):
        # - Set attributes

        self.root = root

        # - Create a single top node or each leaf

        top = Stub("Top")
        for i, leaf in enumerate(self.root.leaves()):
            pipe_connect(leaf, top, f"Top parent {i}")

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
        for method in [
            "update_value",
            "update_pressure",
            "update_speed",
            "update_triggers",
            "update_last_ts",
        ]:
            for node in self.root.iterate("down"):
                getattr(node, method, lambda ts: None)(ts)


def run_fluid_flow(flow: FluidFlow):
    # - Init event manager

    event_manager = SimpleEventManager()

    # - Subscribe to all nodes of the flow

    for node in flow.root.iterate("down"):
        node.set_event_manager(event_manager)
        node.subscribe()

    # - Subscribe to all events for flow.update

    event_manager.subscribe("", flow.update)

    # - Add start event

    event_manager.add_event("update", 0, {})

    # - Run event manager

    event_manager.run()
