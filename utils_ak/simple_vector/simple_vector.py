from utils_ak.clock import *


def cast_simple_vector(obj):
    if isinstance(obj, SimpleVector):
        return obj
    elif isinstance(obj, list):
        return SimpleVector(obj)
    elif isinstance(obj, tuple):
        # todo: del
        # return SimpleVector(list(obj))
        return SimpleVector(obj)
    elif isinstance(obj, int):
        return SimpleVector([0] * obj)
    else:
        raise Exception('Unknown object type')


def cast_vector_values(obj):
    if isinstance(obj, SimpleVector):
        return obj.values
    elif isinstance(obj, list):
        return obj
    elif isinstance(obj, tuple):
        return obj
    elif isinstance(obj, int):
        return [0] * obj
    else:
        raise Exception('Unkown object type')


class SimpleVector:
    def __init__(self, values):
        self.values = cast_vector_values(values)
        self.n_dims = len(self.values)

    def __add__(self, other):
        return SimpleVector([self[i] + other[i] for i in range(self.n_dims)])

    def __sub__(self, other):
        return SimpleVector([self[i] - other[i] for i in range(self.n_dims)])

    def __getitem__(self, item):
        return self.values[item]

    def __repr__(self):
        return 'SimpleVector({})'.format(self.values)

    def __str__(self):
        return 'SimpleVector({})'.format(self.values)

    def __setitem__(self, index, value):
        self.values[index] = value

    def add(self, other):
        for i, v in enumerate(other.values):
            self.values[i] += v
        return self

    def __iter__(self):
        return iter(self.values)

def test_simple_vector():
    v1 = SimpleVector([1, 2])
    v2 = SimpleVector([3, 5])
    print(v1 + v2, v1 - v2)

if __name__ == '__main__':
    test_simple_vector()