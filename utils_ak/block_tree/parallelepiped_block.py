from functools import partial
from utils_ak.simple_vector import *

from utils_ak.block_tree import Block


def relative_acc(parent_props, child_props, key, default=None, formatter=None):
    if callable(default):
        default = default()
    res = child_props.relative_props.get(key, default)
    if formatter:
        res = formatter(res)
    return res


def cumsum_acc(parent_props, child_props, key, default=None, formatter=None):
    if callable(default):
        default = default()

    if parent_props:
        return parent_props[key] + relative_acc(parent_props, child_props, key, default=default, formatter=formatter)

    return relative_acc(parent_props, child_props, key, default=default, formatter=formatter)


class ParallelepipedBlock(Block):
    def __init__(self, block_class, n_dims=2, **props):
        self.n_dims = n_dims
        props.setdefault('props_accumulators', {}).setdefault('x', partial(cumsum_acc, default=lambda: SimpleVector(n_dims), formatter=cast_simple_vector))
        props.setdefault('props_accumulators', {}).setdefault('size', partial(relative_acc, default=lambda: SimpleVector(n_dims), formatter=cast_simple_vector))
        props.setdefault('props_accumulators', {}).setdefault('x_rel', lambda parent_props, child_props, key: relative_acc(parent_props, child_props, 'x', default=lambda: SimpleVector(n_dims), formatter=cast_simple_vector))
        props.setdefault('props_accumulators', {}).setdefault('axis', partial(relative_acc, default=0))
        super().__init__(block_class, **props)

    @property
    def x(self):
        return cast_simple_vector(self.props['x'])

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

    def __repr__(self):
        return self.tabular_str()

    def tabular_str(self, visible_only=False):
        res = ''
        for b in self.iter():
            if b.size[0] != 0:
                if visible_only and b.props['visible'] is False:
                    continue
                res += ' ' * int(b.x[0]) + '=' * int(b.size[0]) + f' {b.props["class"]}' + ' x '.join([f'({b.x[i]}, {b.y[i]}]' for i in range(b.n_dims)])
                res += '\n'
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
        return cast_simple_vector(values)



if __name__ == '__main__':
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