from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests
from utils_ak.block_tree import init_block_maker
from utils_ak.fluid_flow.actors.container import Container
from utils_ak.fluid_flow.actors.hub import Hub
from utils_ak.fluid_flow.actors.pipe import pipe_connect, pipe_disconnect
from utils_ak.fluid_flow.actors.processor import Processor
from utils_ak.fluid_flow.actors.queue_actor import Queue
from utils_ak.fluid_flow.actors.tumblered_actor import TumbleredActor
from utils_ak.fluid_flow.fluid_flow import FluidFlow
from utils_ak.numeric import custom_round


def test_sticks_flow():
    # - Init actors

    # -- Drenator

    drenator = Container("Drenator", value=800, max_pressures=[None, None])

    # -- Pizza melting

    melting = TumbleredActor(
        "Tumbler",
        actor=Container(
            "PizzaMelting",
            max_pressures=[200, 200],
            limits=[1000, 1000],
        ),
        tumbler_func=lambda ts: 3 if ts == 0 else (9999999 if ts < 10 else None),  # wait N seconds before melting
    )

    # -- Sticks melting

    sticks = Queue(
        "SticksQueue",
        [
            Container(
                "SticksMelting1",
                max_pressures=[100, 100],
                limits=[100, 100],
            ),
            Container(
                "SticksMelting2",
                max_pressures=[100, 100],
                limits=[1000, 1000],
            ),
        ],
        break_funcs_by_orient={"in": lambda old, new: 1},  # wait 5 minutes before can melt again
    )

    # - Connect actors

    hub = Hub("Hub")
    pipe_connect(drenator, hub)
    pipe_connect(hub, melting)
    pipe_connect(hub, sticks)

    # - Run flow

    assert FluidFlow(drenator).run().state_snapshot() == snapshot(
        {
            "schema": """\
Container (Drenator) -> Hub (Hub) -> [Pipe (2), Pipe (3)]
Pipe (2) -> TumbleredActor (Tumbler) -> [Plug (Bottom)]
Pipe (3) -> Queue (SticksQueue) -> [Plug (Bottom)]
""",
            "str(self)": """\
Flow:
    Queue (SticksQueue): [["SticksMelting1", 100.0], ["SticksMelting2", 300.0]]\
""",
            "nodes": {
                "Container (Drenator)": {
                    "value": 0.0,
                    "df": "[{'index': 'in', 'max_pressure': None, 'limit': None, 'collected': 0.0}, {'index': 'out', 'max_pressure': None, 'limit': None, 'collected': 800.0}]",
                    "transactions": "[[0, 1.0, -100.0], [2.0, 3, -100.0], [3, 5.0, -600.0]]",
                },
                "TumbleredActor (Tumbler)": {
                    "Container (PizzaMelting)": {
                        "value": 400.0,
                        "df": "[{'index': 'in', 'max_pressure': 200, 'limit': 1000, 'collected': 400.0}, {'index': 'out', 'max_pressure': 200, 'limit': 1000, 'collected': 0.0}]",
                        "transactions": "[[3, 5.0, 400.0]]",
                    }
                },
                "Queue (SticksQueue)": {
                    "queue": [
                        {
                            "value": 100.0,
                            "df": "[{'index': 'in', 'max_pressure': 100, 'limit': 100, 'collected': 100.0}, {'index': 'out', 'max_pressure': 100, 'limit': 100, 'collected': 0.0}]",
                            "transactions": "[[0, 1.0, 100.0]]",
                        },
                        {
                            "value": 300.0,
                            "df": "[{'index': 'in', 'max_pressure': 100, 'limit': 1000, 'collected': 300.0}, {'index': 'out', 'max_pressure': 100, 'limit': 1000, 'collected': 0.0}]",
                            "transactions": "[[2.0, 3, 100.0], [3, 5.0, 200.0]]",
                        },
                    ],
                    "breaks": [[1.0, 2.0]],
                },
            },
        }
    )


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
