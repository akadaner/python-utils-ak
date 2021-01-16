from functools import partial
from utils_ak.simple_vector import *
from utils_ak.properties import *
from utils_ak.block_tree import Block

from utils_ak.clock import *


class ParallelepipedBlock(Block):
    def __init__(self, block_class, n_dims=2, **props):
        self.n_dims = n_dims
        self._zero_simple_vector = SimpleVector(n_dims)
        super().__init__(block_class, props_formatters={'x': lambda k, v: SimpleVector(v), 'size': lambda k, v: SimpleVector(v)}, **props)
        self.props.accumulators['x'] = partial(cumsum_acc, default=lambda: SimpleVector(n_dims))
        self.props.accumulators['x_rel'] = lambda parent_props, child_props, key: relative_acc(parent_props, child_props, 'x', default=lambda: SimpleVector(n_dims))
        self.props.accumulators['size'] = partial(relative_acc)
        self.props.accumulators['axis'] = partial(relative_acc, default=0)
        self.props.accumulators['is_parent_node'] = partial(relative_acc, default=False)

    @property
    def x(self):
        # todo: make properly
        if 'x' not in self.props.relative_props:
            self.props.update(x=[0, 0])

        return self.props['x']

    @property
    def y(self):
        return self.x + self.size

    def __str__(self):
        res = self.props['cls'] + ' ' + ' x '.join([f'({self.x[i]}, {self.y[i]}]' for i in range(self.n_dims)])

        for child in self.children:
            for line in str(child).split('\n'):
                if not line:
                    continue
                res += '\n  ' + line
        return res

    def __repr__(self):
        return self.tabular()

    def is_leaf(self):
        return not self.children and not self.props['is_parent_node']

    def tabular(self):
        res = ''
        for b in self.iter():
            if b.size[0] != 0:
                res += ' ' * int(b.x[0]) + '=' * int(b.size[0]) + f' {b.props["cls"]} ' + ' x '.join([f'({b.x[i]}, {b.y[i]}]' for i in range(b.n_dims)])
                res += '\n'
        return res

    @property
    def size(self):
        # todo: make properly
        if 'size' not in self.props.relative_props:
            self.props.update(size=[0, 0])
        size = self.props['size']
        values = []
        for axis in range(self.n_dims):
            if size[axis] == 0:
                if not self.children:
                    values.append(0)
                else:
                    values.append(max([c.y[axis] - self.x[axis] for c in self.children]))
            else:
                values.append(size[axis])
        return SimpleVector(values)


def test_parallelepiped_block():
    a = ParallelepipedBlock('a', n_dims=2, x=[1, 2])
    b = ParallelepipedBlock('b', n_dims=2)
    c = ParallelepipedBlock('c', n_dims=2, x=[3, 4], size=[1, 5])
    a.add_child(b)
    b.add_child(c)

    print(a)
    print(b)
    print(c)
    print(a.x, a.size, a.y)
    print(b.x, b.size, b.y)
    print(c.x, c.size, c.y)

    print()
    print(a['b']['c'])

    print(a.__repr__())


if __name__ == '__main__':
    test_parallelepiped_block()