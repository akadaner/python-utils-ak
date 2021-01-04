from utils_ak.portion import *
from utils_ak.serialization import cast_js
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock


def validate_disjoint_by_axis(b1, b2, axis=0):
    try:
        disposition = int(b1.y[axis] - b2.x[axis])
    except:
        disposition = 1

    i1 = cast_interval(b1.x[axis], b1.y[axis])
    i2 = cast_interval(b2.x[axis], b2.y[axis])

    assert calc_interval_length(i1 & i2) == 0, cast_js({'disposition': disposition})


class ClassValidator:
    def __init__(self, window=2):
        self.validators = {}
        self.window = window

    def add(self, class1, class2, validator, uni_direction=False):
        self.validators[(class1, class2)] = validator
        if uni_direction:
            self.validators[(class2, class1)] = validator

    def validate(self, b1, b2):
        key = (b1.props['class'], b2.props['class'])
        if key in self.validators:
            self.validators[key](b1, b2)

    def __call__(self, parent, block):
        parent_blocks = parent.children[-self.window:]
        if not parent_blocks:
            return

        b2 = block
        for b1 in parent_blocks:
            self.validate(b1, b2)


if __name__ == '__main__':
    print('Validate disjoint test')
    for t in range(0, 10):
        a = ParallelepipedBlock('a', n_dims=1, x=[t], size=[4])
        b = ParallelepipedBlock('b', n_dims=1, x=[3], size=[3])
        print(a, b)
        try:
            validate_disjoint_by_axis(a, b, 0)
        except AssertionError as e:
            print('AssertionError on disposition', e)

    # todo: class_validator_test