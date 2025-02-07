from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.hub import Hub
from utils_ak.fluid_flow.actors.pipe import pipe_connect
from utils_ak.fluid_flow.actors.processor import Processor
from utils_ak.fluid_flow.actors.queue_actor import Queue
from utils_ak.fluid_flow.fluid_flow import FluidFlow


def test_misc():
    # - Test 1

    parent = Container("Parent", value=100, max_pressures=[None, 20])

    child1 = Processor("Child1", max_pressures=[20, None], lag=5, limits=[40, None])
    child2 = Processor("Child2", max_pressures=[10, None], lag=5, limits=[50, None])

    queue = Queue("Queue", [child1, child2])

    pipe_connect(parent, queue)

    assert FluidFlow(parent).run().state_snapshot() == snapshot(
        {
            "schema": """\
    Container (Parent) -> Pipe (3) -> Queue (Queue) -> Pipe (4) -> Stub (Top) -> [None]
    """,
            "str(flow)": """\
    Flow:
        Container (Parent): 10.0
        Queue (Queue): [["Child1", [0.0, 40.0]], ["Child2", [0.0, 50.0]]]\
    """,
            "nodes": {
                "Container (Parent)": {
                    "value": 10.0,
                    "df": "[{'index': 'in', 'max_pressure': nan, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': 20.0, 'limit': None, 'collected': 90.0}]",
                    "transactions": "[[0, 2.0, -40.0], [2.0, 5, -30.0], [5.0, 7.0, -20.0]]",
                },
                "Queue (Queue)": {
                    "queue": [
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': 20.0, 'limit': 40.0, 'collected': 40.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 40.0}]",
                                "transactions": "[[0, 2.0, 40.0], [5.0, 7.0, -40.0]]",
                            },
                            "out": {
                                "value": 40.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 40.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                                "transactions": "[[5.0, 7.0, 40.0]]",
                            },
                        },
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': 10.0, 'limit': 50.0, 'collected': 50.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 50.0}]",
                                "transactions": "[[2.0, 5, 30.0], [5.0, 7.0, 20.0], [7.0, 8.0, -10.0], [8.0, 12.0, -40.0]]",
                            },
                            "out": {
                                "value": 50.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 50.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                                "transactions": "[[7.0, 8.0, 10.0], [8.0, 12.0, 40.0]]",
                            },
                        },
                    ],
                    "breaks": [],
                },
            },
        }
    )

    # - Test 2 with different items

    parent1 = Container("Parent1", item="a", value=100, max_pressures=[None, 10], limits=[None, 100])
    parent2 = Container("Parent2", item="b", value=100, max_pressures=[None, 20], limits=[None, 100])
    queue = Queue("Parent", [parent1, parent2])

    hub = Hub("hub")
    child1 = Container("Child1", item="a", max_pressures=[None, None])
    child2 = Container("Child2", item="b", max_pressures=[None, None])

    pipe_connect(queue, hub, "parent-hub")
    pipe_connect(hub, child1, "hub-child1")
    pipe_connect(hub, child2, "hub-child2")

    assert FluidFlow(queue).run().state_snapshot() == snapshot(
        {
            "schema": """\
    Queue (Parent) -> Pipe (parent-hub) -> Hub (hub) -> [Pipe (hub-child1), Pipe (hub-child2)]
    Pipe (hub-child1) -> Container (Child1:a) -> Pipe (5) -> [Stub (Top)]
    Pipe (hub-child2) -> Container (Child2:b) -> Pipe (6) -> [Stub (Top)]
    Stub (Top) -> [None]
    """,
            "str(flow)": """\
    Flow:
        Queue (Parent): [["Parent1", 0.0], ["Parent2", 0.0]]
        Container (Child1:a): 100.0
        Container (Child2:b): 100.0\
    """,
            "nodes": {
                "Queue (Parent)": {
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
                "Container (Child1:a)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[0, 10.0, 100.0]]",
                },
                "Container (Child2:b)": {
                    "value": 100.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 100.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                    "transactions": "[[10.0, 15.0, 100.0]]",
                },
            },
        }
    )

if __name__ == '__main__':
    run_inline_snapshot_tests(mode='update_all')