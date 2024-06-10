from utils_ak.block_tree.validation.validate_disjoint_by_intervals import validate_disjoint_by_intervals


def validate_order_by_axis(b1, b2, axis=0, equal_allowed=False):
    i1_single = (
        (b1.x[axis] - 1, b1.x[axis]) if equal_allowed else (b1.x[axis], b1.x[axis] + 1)
    )  # if equal allowed - start on one position earlier
    i2_single = (b2.x[axis], b2.x[axis] + 1)
    validate_disjoint_by_intervals(
        i1_single,
        i2_single,
        distance=0,
        ordered=True,
    )

