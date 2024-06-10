from utils_ak.block_tree import ParallelepipedBlock
from utils_ak.block_tree.validation.validate_disjoint import validate_disjoint


def is_disjoint(b1: ParallelepipedBlock, b2: ParallelepipedBlock):
    """Check if two rectangles are disjoint."""

    def _is_disjoint_by_axis(b1, b2, axis=0):
        try:
            validate_disjoint(b1, b2, axis)
            return True
        except:
            return False

    return any(_is_disjoint_by_axis(b1, b2, axis) for axis in range(b1.n_dims))

