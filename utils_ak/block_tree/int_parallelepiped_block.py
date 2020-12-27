import numpy as np
from functools import partial
from utils_ak.properties import *
from utils_ak.numeric import is_numeric

from utils_ak.block_tree import Block


def cumsum_acc(parent_props, child_props, key, default=None, formatter=None):
    pv, v = cast_prop_values(parent_props, child_props, key)

    if callable(default):
        default = default()

    pv = pv if pv is not None else default
    v = v if v is not None else default

    formatter = formatter or (lambda parent_props, child_props, x: x)
    pv, v = formatter(parent_props, child_props, pv), formatter(parent_props, child_props, v)
    return pv + v


def relative_acc(parent_props, child_props, key, default=None):
    if callable(default):
        default = default()
    return child_props.relative_props.get(key, default)


class IntParallelepipedBlock(Block):
    def __init__(self, block_class, n_dims=2, **props):
        self.n_dims = n_dims
        props.setdefault('props_accumulators', {}).setdefault('x', partial(cumsum_acc, default=lambda: np.zeros(n_dims).astype(int), formatter=lambda parent_props, child_props, v: np.array(v)))
        props.setdefault('props_accumulators', {}).setdefault('size', partial(relative_acc, default=lambda: np.zeros(n_dims).astype(int)))
        props.setdefault('props_accumulators', {}).setdefault('x_rel', partial(relative_acc, default=lambda: np.zeros(n_dims).astype(int)))
        props.setdefault('props_accumulators', {}).setdefault('axis', partial(relative_acc, default=0))
        super().__init__(block_class, **props)

    @property
    def x(self):
        return self.props['x']

    @property
    def y(self):
        return self.x + self.size

    def __str__(self):
        res = self.props["class"] + ' ' + ' x '.join([f'({self.x[i]}, {self.y[i]}]' for i in range(self.n_dims)])

        for child in self.children:
            for line in str(child).split('\n'):
                if not line:
                    continue
                res += '\n  ' + line
        return res

    @property
    def size(self):
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
        return np.array(values)


if __name__ == '__main__':
    a = IntParallelepipedBlock('a', n_dims=2, x=np.array([1, 2]))
    b = IntParallelepipedBlock('b', n_dims=2)
    c = IntParallelepipedBlock('c', n_dims=2, x=np.array([3, 4]), size=np.array([1, 5]))
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
