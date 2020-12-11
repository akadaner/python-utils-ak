def cast_prop_values(parent, child, key):
    if not parent:
        pv = None
    else:
        pv = parent[key]

    v = child.relative_props.get(key)
    return pv, v


class MixedProps:
    def __init__(self, props=None, dynamic_accumulators=None, dynamic_keys=None,
                 static_accumulators=None, required_static_keys=None):

        self.relative_props = props or {}
        self.static_props = {}

        self.dynamic_accumulators = dynamic_accumulators or {}
        # use accumulator keys as default dynamic keys
        self.dynamic_keys = dynamic_keys or list(self.dynamic_accumulators.keys())

        self.static_accumulators = static_accumulators
        self.required_static_keys = required_static_keys or []

        self.parent = None
        self.children = []

    @staticmethod
    def default_accumulator(parent, child, key):
        pv, v = cast_prop_values(parent, child, key)
        return v if v is not None else pv

    def update(self, props, accumulate=False):
        self.relative_props.update(props)
        if accumulate:
            if accumulate is True:
                keys = props.keys()
            elif isinstance(accumulate, list):
                keys = accumulate
            else:
                raise Exception('Unknown accumulation requested')
            self.accumulate_static(keys=keys)

    def accumulate_static(self, keys=None, recursive=False):
        new_keys = keys or []
        new_keys = list(new_keys)

        if not new_keys:
            # reset static props
            self.static_props = {}

        parent_static_props = {} if not self.parent else self.parent.static_props

        all_relevant_keys = list(parent_static_props.keys()) + list(self.relative_props.keys()) + self.required_static_keys
        all_relevant_keys = set(all_relevant_keys)
        all_relevant_keys = [key for key in all_relevant_keys if key not in self.dynamic_keys]

        # if keys present - filter only necessary keys to accumulate
        new_keys = all_relevant_keys if not new_keys else [key for key in new_keys if key in all_relevant_keys]

        # update static keys
        for key in new_keys:
            accumulator = self.static_accumulators.get(key, self.default_accumulator)
            self.static_props[key] = accumulator(self.parent, self, key)

        if recursive:
            for child in self.children:
                child.accumulate_static(keys=keys, recursive=recursive)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def __getitem__(self, item):
        if item in self.dynamic_keys:
            accumulator = self.dynamic_accumulators.get(item, self.default_accumulator)
            return accumulator(self.parent, self, item)
        elif item in self.static_props:
            return self.static_props[item]
        elif item in self.relative_props and item not in self.static_accumulators:
            # item would be in static props if accumulated
            return self.relative_props[item]

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


    DYNAMIC_ACCUMULATORS = {'t': t_acc}
    STATIC_ACCUMULATORS = {'size': size_acc, 'time_size': time_size_acc}


    def gen_props(props=None):
        return MixedProps(props=props, dynamic_accumulators=DYNAMIC_ACCUMULATORS,
                          static_accumulators=STATIC_ACCUMULATORS,
                          required_static_keys=['size', 'time_size'], dynamic_keys=['t', 'b'])

    root = gen_props({'t': 1, 'size': 5, 'a': 'sadf', 'd': 1})
    child1 = gen_props({'t': 2})
    child2 = gen_props({'t': 3})
    root.add_child(child1)
    child1.add_child(child2)

    print(root['a'])

    root.accumulate_static(recursive=True)

    print(root.static_props, child1.static_props, root['t'], child1['t'])
    print(root.get('bad key', 'bad key default'))

    root.update({'b': 5})
    print(child1['b'])
    print(child2['b'])

    root.update({'c': 6, 'd': 7})
    root.accumulate_static(['d'], recursive=True)
    print(child2.static_props)