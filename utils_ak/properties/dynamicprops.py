from utils_ak.interactive_imports import *


def cast_prop_values(parent, child, key):
    if not parent:
        pv = None
    else:
        pv = parent[key]

    v = child.relative_props.get(key)
    return pv, v


class DynamicProps:
    def __init__(self, props=None, accumulators=None, required_keys=None):
        self.relative_props = props or {}
        self.accumulators = accumulators
        self.required_keys = required_keys or []

        self.parent = None
        self.children = []

    @staticmethod
    def default_accumulator(parent, child, key):
        pv, v = cast_prop_values(parent, child, key)
        return v if v is not None else pv

    def update(self, props):
        self.relative_props.update(props)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def __getitem__(self, item):
        accumulator = self.accumulators.get(item, self.default_accumulator)
        return accumulator(self.parent, self, item)

    def get(self, item, default=None):
        res = self[item]
        if res is None:
            res = default
        return res


if __name__ == '__main__':
    def t_acc(parent, child, key):
        pv, v = cast_prop_values(parent, child, key)
        pv = pv if pv else 0
        v = v if v else 0
        return pv + v


    def size_acc(parent, child, key):
        size, time_size = child.relative_props.get('size', 0), child.relative_props.get('time_size', 0)
        size, time_size = int(size), int(time_size)
        if size:
            return int(size)
        else:
            assert time_size % 5 == 0
            return time_size // 5


    def time_size_acc(parent, child, key):
        size, time_size = child.relative_props.get('size', 0), child.relative_props.get('time_size', 0)
        size, time_size = int(size), int(time_size)
        if size:
            return size * 5
        else:
            return time_size


    ACCUMULATORS = {'t': t_acc, 'size': size_acc, 'time_size': time_size_acc}

    def gen_props(props=None):
        return DynamicProps(props=props, accumulators=ACCUMULATORS, required_keys=['size', 'time_size'])

    root = gen_props({'t': 1, 'size': 5, 'other': 1})
    child1 = gen_props({'t': 2})
    child2 = gen_props({'t': 3})
    root.add_child(child1)
    child1.add_child(child2)

    values = []
    for i, node in enumerate([root, child1, child2]):
        for key in ['t', 'size', 'time_size', 'other', 'non-existent_key']:
            values.append([i, key, node[key]])

    print(pd.DataFrame(values))