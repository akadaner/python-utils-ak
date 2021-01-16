from utils_ak.portion import *
from utils_ak.serialization import cast_js
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block


def validate_disjoint_by_axis(b1, b2, axis=0):
    try:
        disposition = int(b1.y[axis] - b2.x[axis])
    except:
        disposition = 1

    i1 = cast_interval(b1.x[axis], b1.y[axis])
    i2 = cast_interval(b2.x[axis], b2.y[axis])

    assert calc_interval_length(i1 & i2) == 0, cast_js({'msg': 'Failed to validate disjoint between {}:{} and {}:{}'.format(b1.props['cls'], i1, b2.props['cls'], i2), 'disposition': disposition})


def test_validate_disjoint_by_axis():
    print('Validate disjoint test')
    for t in range(0, 10):
        a = ParallelepipedBlock('a', n_dims=1, x=[t], size=[4])
        b = ParallelepipedBlock('b', n_dims=1, x=[3], size=[3])
        print(a, b)
        try:
            validate_disjoint_by_axis(a, b, 0)
        except AssertionError as e:
            print('AssertionError on disposition', e)


class ClassValidator:
    def __init__(self, window=2):
        self.validators = {}
        self.window = window

    def add(self, class1, class2, validator, uni_direction=False):
        self.validators[(class1, class2)] = validator
        if uni_direction:
            self.validators[(class2, class1)] = validator

    def validate(self, b1, b2):
        key = (b1.props['cls'], b2.props['cls'])
        if key in self.validators:
            self.validators[key](b1, b2)

    def __call__(self, parent, block):
        parent_blocks = parent.children[-self.window:]

        if not parent_blocks:
            return

        b2 = block
        for b1 in parent_blocks:
            self.validate(b1, b2)


def test_class_validator():
    class_validator = ClassValidator(window=1)
    class_validator.add('a', 'a', validator=validate_disjoint_by_axis)

    root = ParallelepipedBlock('root', axis=0)
    a1 = ParallelepipedBlock('a', n_dims=2, x=[0, 0], size=[5, 1])
    a2 = ParallelepipedBlock('a', n_dims=2, x=[2, 0], size=[5, 1])
    b = ParallelepipedBlock('b', n_dims=2, x=[0, 1], size=[5, 1])
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


if __name__ == '__main__':
    test_validate_disjoint_by_axis()
    test_class_validator()