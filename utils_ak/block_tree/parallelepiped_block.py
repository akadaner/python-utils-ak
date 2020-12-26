import numpy as np
from functools import partial
from utils_ak.properties import *


from .block import Block


def cumsum_acc(parent, child, key, default):
    pv, v = cast_prop_values(parent, child, key)
    pv = pv if pv else default()
    v = v if v else default()
    return pv + v


# todo: make sure than np.arrays are fast
class ParallelepipedBlock(Block):
    def __init__(self, n_dims=2, **kwargs):
        self.n_dims = n_dims
        kwargs.setdefault('props_accumulators', {}).setdefault('x', partial(cumsum_acc, default=lambda: np.zeros(n_dims)))
        super().__init__(**kwargs)

    @property
    def x(self):
        return self.props['x']

    @property
    def y(self):
        return self.x + self.size

    def __str__(self):
        res = self.props["class"] + 'x'.join([f'({self.x[i]}, {self.y[i]}]]\n' for i in range(self.n_dims)])

        for child in self.children:
            for line in str(child).split('\n'):
                if not line:
                    continue
                res += '  ' + line + '\n'
        return res

    @property
    def size(self):
        size = self.props['size']

        if size is not None:
            return size
        else:
            # no size - get size from children!
            if not self.children:
                return np.zeros(self.n_dims)
            else:
                # orient is the same as in the children
                values = []
                for i in range(self.n_dims):
                    values.append(max([0] + [c.y[i] - self.x[i] for c in self.children]))
                return np.array(values)
