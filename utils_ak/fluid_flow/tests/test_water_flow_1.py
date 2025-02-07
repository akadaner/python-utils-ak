import json

from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.block_tree import init_block_maker
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.hub import Hub
from utils_ak.fluid_flow.actors.pipe import pipe_connect
from utils_ak.fluid_flow.actors.processor import Processor
from utils_ak.fluid_flow.actors.queue_actor import Queue
from utils_ak.fluid_flow.actors.sequence import Sequence
from utils_ak.fluid_flow.fluid_flow import FluidFlow
from utils_ak.numeric import custom_round


def test_water_flow_1():
    # - Define actors

    # -- Drenator

    drenator = Container("Drenator", value=1000, max_pressures=[None, None])

    # -- Melting queue

    melting_queue = Queue(
        "MeltingQueue",
        [
            Processor(
                "Melting1",
                items=["a", "a"],
                max_pressures=[1000, 1000],
                lag=0,
                limits=[750, 750],
            ),
            Processor(
                "Melting2",
                items=["b", "b"],
                max_pressures=[1000, 1000],
                lag=0,
                limits=[250, 250],
            ),
        ],
        break_funcs_by_orient={"in": lambda old, new: 1},
    )

    # -- Cooling queue

    cooling_queue = Queue(
        "CoolingQueue",
        [
            Processor(
                "Cooling1",
                items=["a", "a"],
                max_pressures=[None, None],
                lag=0.5,
                limits=[750, 750],
            ),
            Processor(
                "Cooling2",
                items=["b", "b"],
                max_pressures=[None, None],
                lag=0.5,
                limits=[250, 250],
            ),
        ],
    )

    # -- Packing Hub

    packing_hub = Hub("Hub")

    # -- Packing queue1

    packing_queue1 = Queue(
        "PackingQueue1",
        [
            Processor(
                "Packing1",
                items=["a", "a1"],
                max_pressures=[200, None],
                lag=0,
                limits=[750, None],
            ),
            Processor(
                "Packing2",
                items=["b", "b1"],
                max_pressures=[400, None],
                lag=0,
                limits=[100, None],
            ),
        ],
    )

    # using sequence as packing
    container = Container("Container3", item="b", max_pressures=[200, None], limits=[150, None])

    # slow packing
    processor = Processor("Processor3", items=["b", "b2"], max_pressures=[50, None], lag=0)
    packing3 = Sequence("Packing3", [container, processor])
    packing_queue2 = Queue("PackingQueue1", [packing3])

    # - Connect actors

    pipe_connect(drenator, melting_queue)
    pipe_connect(melting_queue, cooling_queue)
    pipe_connect(cooling_queue, packing_hub)

    pipe_connect(packing_hub, packing_queue1)
    pipe_connect(packing_hub, packing_queue2)

    # - Run flow

    flow = FluidFlow(drenator).run()

    assert flow.state_snapshot() == snapshot(
        {
            "schema": """\
Container (Drenator) -> Pipe (drenator-melting) -> Queue (MeltingQueue) -> Pipe (melting-cooling) -> Queue (CoolingQueue) -> Pipe (cooling-hub) -> Hub (Hub) -> [Pipe (hub-packing_queue1), Pipe (hub-packing_queue2)]
Pipe (hub-packing_queue1) -> Queue (PackingQueue1) -> Pipe (2) -> [Stub (Bottom)]
Pipe (hub-packing_queue2) -> Queue (PackingQueue1) -> Pipe (3) -> [Stub (Bottom)]
Stub (Bottom) -> [None]
""",
            "str(flow)": """\
Flow:
    Queue (MeltingQueue): [["Melting1", [0.0, 0.0]], ["Melting2", [0.0, 0.0]]]
    Queue (CoolingQueue): [["Cooling1", [0.0, 0.0]], ["Cooling2", [0.0, 0.0]]]
    Queue (PackingQueue1): [["Packing1", [0.0, 750.0]], ["Packing2", [0.0, 100.0]]]
    Queue (PackingQueue1): [["Packing3", [0.0, [0.0, 150.0]]]]\
""",
            "nodes": {
                "Container (Drenator)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 1000.0}]",
                    "transactions": "[[0, 0.5, -500.0], [0.5, 0.75, -250.0], [1.75, 2.0, -250.0]]",
                },
                "Queue (MeltingQueue)": {
                    "queue": [
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': 1000.0, 'limit': 750.0, 'collected': 750.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 750.0}]",
                                "transactions": "[[0, 0.5, 500.0], [0, 0.5, -500.0], [0.5, 0.75, 250.0], [0.5, 0.75, -250.0]]",
                            },
                            "out": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 750.0}, {'index': 'out', 'max_pressure': 1000.0, 'limit': 750.0, 'collected': 750.0}]",
                                "transactions": "[[0, 0.5, 500.0], [0, 0.5, -500.0], [0.5, 0.75, 250.0], [0.5, 0.75, -250.0]]",
                            },
                        },
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': 1000.0, 'limit': 250.0, 'collected': 250.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 250.0}]",
                                "transactions": "[[1.75, 2.0, 250.0], [1.75, 2.0, -250.0]]",
                            },
                            "out": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': nan, 'limit': nan, 'collected': 250.0}, {'index': 'out', 'max_pressure': 1000.0, 'limit': 250.0, 'collected': 250.0}]",
                                "transactions": "[[1.75, 2.0, 250.0], [1.75, 2.0, -250.0]]",
                            },
                        },
                    ],
                    "breaks": [[0.75, 1.75]],
                },
                "Queue (CoolingQueue)": {
                    "queue": [
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': 750.0, 'collected': 750.0}, {'index': 'out', 'max_pressure': None, 'limit': nan, 'collected': 750.0}]",
                                "transactions": "[[0, 0.5, 500.0], [0.5, 0.75, 250.0], [0.5, 0.75, -250.0], [0.75, 1.0, -250.0], [1.0, 1.25, -250.0]]",
                            },
                            "out": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': nan, 'collected': 750.0}, {'index': 'out', 'max_pressure': None, 'limit': 750.0, 'collected': 750.0}]",
                                "transactions": "[[0.5, 0.75, 250.0], [0.5, 0.75, -50.0], [0.75, 1.0, 250.0], [0.75, 1.0, -50.0], [1.0, 1.25, 250.0], [1.0, 1.25, -50.0], [1.25, 1.75, -100.0], [1.75, 2.0, -50.0], [2.0, 2.25, -50.0], [2.25, 2.5, -50.0], [2.5, 4.25, -350.0]]",
                            },
                        },
                        {
                            "in": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': 250.0, 'collected': 250.0}, {'index': 'out', 'max_pressure': None, 'limit': nan, 'collected': 250.0}]",
                                "transactions": "[[1.75, 2.0, 250.0], [2.25, 2.5, -250.0]]",
                            },
                            "out": {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': None, 'limit': nan, 'collected': 250.0}, {'index': 'out', 'max_pressure': None, 'limit': 250.0, 'collected': 250.0}]",
                                "transactions": "[[2.25, 2.5, 250.0], [4.25, 4.5, -150.0], [4.5, 4.666666666666667, -33.33333333333339], [4.666666666666667, 5.0, -66.6666666666666]]",
                            },
                        },
                    ],
                    "breaks": [],
                },
                "Queue (PackingQueue1)": {
                    "queue": [
                        [
                            {
                                "value": 0.0,
                                "df": "[{'index': 'in', 'max_pressure': 200.0, 'limit': 150.0, 'collected': 150.0}, {'index': 'out', 'max_pressure': nan, 'limit': nan, 'collected': 150.0}]",
                                "transactions": "[[4.25, 4.5, 50.0], [4.25, 4.5, -12.5], [4.5, 4.666666666666667, 33.33333333333339], [4.5, 4.666666666666667, -8.333333333333348], [4.666666666666667, 5.0, 66.6666666666666], [4.666666666666667, 5.0, -16.66666666666665], [5.0, 7.25, -112.5]]",
                            },
                            {},
                            {
                                "in": {
                                    "value": 0.0,
                                    "df": "[{'index': 'in', 'max_pressure': 50.0, 'limit': None, 'collected': 150.0}, {'index': 'out', 'max_pressure': nan, 'limit': None, 'collected': 150.0}]",
                                    "transactions": "[[4.25, 4.5, 12.5], [4.25, 4.5, -12.5], [4.5, 4.666666666666667, 8.333333333333348], [4.5, 4.666666666666667, -8.333333333333348], [4.666666666666667, 5.0, 16.66666666666665], [4.666666666666667, 5.0, -16.66666666666665], [5.0, 7.25, 112.5], [5.0, 7.25, -112.5]]",
                                },
                                "out": {
                                    "value": 150.0,
                                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 150.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 0.0}]",
                                    "transactions": "[[4.25, 4.5, 12.5], [4.5, 4.666666666666667, 8.333333333333348], [4.666666666666667, 5.0, 16.66666666666665], [5.0, 7.25, 112.5]]",
                                },
                            },
                        ]
                    ],
                    "breaks": [],
                },
            },
        }
    )

    # - Build blocks

    maker, make = init_block_maker("root", axis=1)

    for node in drenator.iterate("down"):
        if node.active_periods():
            for period in node.active_periods():
                label = "-".join([str(node.name), period[0]])
                beg, end = period[1:]
                beg, end = (
                    custom_round(beg * 60, 5) // 5,
                    custom_round(end * 60, 5) // 5,
                )
                make(label, x=[beg, 0], size=(end - beg, 1))

    assert str(maker.root) == snapshot("""\
root (-, 0:07:15] x (-, 8]
  Drenator-default (-, 0:02:00] x (-, 1]
  MeltingQueue-a (-, 0:00:45] x (1, 2]
  MeltingQueue-b (0:01:45, 0:02:00] x (2, 3]
  CoolingQueue-a (-, 0:01:15] x (3, 4]
  CoolingQueue-b (0:01:45, 0:02:30] x (4, 5]
  PackingQueue1-a (0:00:30, 0:04:15] x (5, 6]
  PackingQueue1-b (0:04:15, 0:04:30] x (6, 7]
  PackingQueue1-b (0:04:15, 0:07:15] x (7, 8]\
""")


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
