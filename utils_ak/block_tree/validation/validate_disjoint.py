from utils_ak.block_tree import ParallelepipedBlock
from utils_ak.block_tree.validation.validate_disjoint_by_intervals import validate_disjoint_by_intervals
from utils_ak.lazy_tester import lazy_tester

import json
from loguru import logger


def validate_disjoint(
    b1: ParallelepipedBlock,
    b2: ParallelepipedBlock,
    axis: int = 0,
    distance: int = 0,
    ordered: bool = False,
) -> None:
    """
    Raises AssertionError if two rectangles are not disjoint by axis

    Parameters
    ----------
    b1 : ParallelepipedBlock
        First block
    b2 : ParallelepipedBlock
        Second block
    axis : int, optional
        0=x, 1=y
    distance : int, optional
        Distance between blocks. For example, `validate_disjoint(a, b, distance=1)` will check if a and b are disjoint and their distance is at least 1.
    ordered : bool, optional
        Forces b1 to be on the left of b2

    Raises
    ------
    AssertionError
    If two rectangles are not disjoint, an AssertionError is raised with the disposition of the blocks: `raise AssertionError(cast_js({"disposition": disposition}))`

    This disposition is used in `AxisPusher` to insert blocks efficiently.

    """
    try:
        validate_disjoint_by_intervals(
            (b1.x[axis], b1.y[axis]),
            (b2.x[axis], b2.y[axis]),
            distance=distance,
            ordered=ordered,
        )
    except AssertionError as e:
        # [DISJOINT TRACE]
        logger.trace(
            "Disjoint",
            b1=b1.props["cls"],
            b1_boiling_id=b1.props["boiling_id"],
            b2=b2.props["cls"],
            b2_boiling_id=b2.props["boiling_id"],
            disposition=json.loads(str(e))["disposition"],
            axis=axis,
            distance=distance,
            ordered=ordered,
        )
        raise


def test():
    lazy_tester.log("Basic test")
    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)
        try:
            validate_disjoint(a, b, 0)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)

    lazy_tester.log("\n")
    lazy_tester.log("Distance = 1")

    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)

        try:
            validate_disjoint(a, b, 0, distance=1)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)

    lazy_tester.log("\n")
    lazy_tester.log("Ordered")

    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)

        try:
            validate_disjoint(a, b, 0, ordered=True)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)
    lazy_tester.assert_logs(reset=True)
