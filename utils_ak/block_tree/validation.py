from utils_ak.portion import *
from utils_ak.coder import cast_js
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block

from loguru import logger


def validate_disjoint_by_axis(b1, b2, axis=0):
    validate_disjoint_by_intervals((b1.x[axis], b1.y[axis]), (b2.x[axis], b2.y[axis]))


def validate_disjoint_by_intervals(i1, i2):
    try:
        disposition = int(i1[1] - i2[0])
    except:
        disposition = 1

    i1 = cast_interval(*i1)
    i2 = cast_interval(*i2)

    assert calc_interval_length(i1 & i2) == 0, cast_js({"disposition": disposition})


def test_validate_disjoint_by_axis():
    print("Validate disjoint test")
    for t in range(0, 10):
        a = ParallelepipedBlock("a", n_dims=1, x=[t], size=[4])
        b = ParallelepipedBlock("b", n_dims=1, x=[3], size=[3])
        print(a, b)
        try:
            validate_disjoint_by_axis(a, b, 0)
        except AssertionError as e:
            print("AssertionError on disposition", e)


class ClassValidator:
    def __init__(self, window=2, window_by_classes=None):
        self.validators = {}
        self.window = window
        self.window_by_classes = window_by_classes or {}

    def add(self, class1, class2, validator, uni_direction=False):
        self.validators[(class1, class2)] = validator
        if uni_direction:
            self.validators[(class2, class1)] = validator

    def validate(self, b1, b2):
        key = (b1.props["cls"], b2.props["cls"])
        if key in self.validators:
            self.validators[key](b1, b2)

    def __call__(self, parent, block):
        parent_blocks = parent.children[-self.window :]

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


if __name__ == "__main__":
    test_validate_disjoint_by_axis()
    test_class_validator()
