from utils_ak.portion import *
from utils_ak.coder import cast_js
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block
from utils_ak.clock import *
from utils_ak.str import *

from loguru import logger

from utils_ak.lazy_tester import lazy_tester


def validate_disjoint_by_axis(b1, b2, axis=0, distance=0, ordered=False):
    validate_disjoint_by_intervals(
        (b1.x[axis], b1.y[axis]),
        (b2.x[axis], b2.y[axis]),
        distance=distance,
        ordered=ordered,
    )


def is_disjoint(b1, b2):
    """ Check if two rectangles are disjoint. """
    def _is_disjoint_by_axis(b1, b2, axis=0):
        try:
            validate_disjoint_by_axis(b1, b2, axis)
            return True
        except:
            return False
    return any(_is_disjoint_by_axis(b1, b2, axis) for axis in range(b1.n_dims))


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


def _calc_interval_intersection(i1, i2):
    return max(min(i1[1], i2[1]) - max(i1[0], i2[0]), 0)


def validate_disjoint_by_intervals(i1, i2, distance=0, ordered=False):
    """
    :param i1:
    :param i2:
    :param distance:
    :param ordered: i1 <= i2
    :return:
    """

    # add neighborhood to the first interval
    i1 = (i1[0] - distance, i1[1] + distance)

    if i1[0] >= i2[0]:
        return

    if _calc_interval_intersection(i1, i2) != 0 or (ordered and i2[0] <= i1[0]):
        try:
            disposition = int(i1[1] - i2[0])
        except:
            disposition = 1

        raise AssertionError(cast_js({"disposition": disposition}))


def test_validate_disjoint_by_axis():
    lazy_tester.log("Basic test")
    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)
        try:
            validate_disjoint_by_axis(a, b, 0)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)

    lazy_tester.log("\n")
    lazy_tester.log("Distance = 1")

    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)

        try:
            validate_disjoint_by_axis(a, b, 0, distance=1)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)

    lazy_tester.log("\n")
    lazy_tester.log("Ordered")

    for t in range(10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[3])
        b = ParallelepipedBlock("b", n_dims=1, x=[5], size=[3])
        lazy_tester.log("Blocks", a=a, b=b)

        try:
            validate_disjoint_by_axis(a, b, 0, ordered=True)
        except AssertionError as e:
            lazy_tester.log("AssertionError on disposition", e=e)
    lazy_tester.assert_logs(reset=True)


def disjoint_validator(parent, block):
    axis = parent.props["axis"]
    for c in parent.children[:-1]:
        validate_disjoint_by_axis(c, block, axis)


class ClassValidator:
    def __init__(self, window=2, window_by_classes=None):
        self.validators = {}
        self.window = window
        self.window_by_classes = window_by_classes or {}

        # add attribute validations
        validate_attrs = [attr for attr in dir(self) if attr.startswith("validate__")]

        for attr in validate_attrs:
            class1, class2 = trim(attr, "validate__").split("__")
            self.add(class1, class2, getattr(self, attr))

    def add(self, class1, class2, validator):
        self.validators[(class1, class2)] = validator

    def validate(self, b1, b2):
        key = (b1.props["cls"], b2.props["cls"])
        if key in self.validators:
            self.validators[key](b1, b2)

    def __call__(self, parent, block):
        parent_blocks = parent.children[-self.window - 1 : -1]

        if not parent_blocks:
            return

        b2 = block

        if not self.window_by_classes.get(block.props["cls"]):
            for b1 in parent_blocks:
                self.validate(b1, b2)
        else:
            parent_classes = set([b.props["cls"] for b in parent_blocks])
            for parent_cls in parent_classes:
                if parent_cls not in self.window_by_classes[block.props["cls"]]:
                    # don't check at all
                    continue
                cls_parent_blocks = [
                    b for b in parent_blocks if b.props["cls"] == parent_cls
                ]
                cls_parent_blocks = cls_parent_blocks[
                    -self.window_by_classes[block.props["cls"]][parent_cls] :
                ]

                for b1 in cls_parent_blocks:
                    self.validate(b1, b2)


def test_class_validator():
    class_validator = ClassValidator(window=1)
    class_validator.add("a", "a", validator=validate_disjoint_by_axis)

    root = ParallelepipedBlock("root", axis=0)
    a1 = ParallelepipedBlock("a", n_dims=2, x=[0, 0], size=[5, 1])
    a2 = ParallelepipedBlock("a", n_dims=2, x=[2, 0], size=[5, 1])
    b = ParallelepipedBlock("b", n_dims=2, x=[0, 1], size=[5, 1])
    root.add_child(a1)

    try:
        class_validator(root, a2)
    except AssertionError as e:
        print(e)

    class_validator(root, b)

    root.add_child(b)

    # window is 1 - validation should pass now
    class_validator(root, a2)

    root.add_child(a2)


def test_interval_intersection():
    print(_calc_interval_intersection([0, 2], [3, 4]))
    print(_calc_interval_intersection([0, 2], [1, 3]))


if __name__ == "__main__":
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout()
    lazy_tester.verbose = True
    test_validate_disjoint_by_axis()
    # test_class_validator()
    test_interval_intersection()
