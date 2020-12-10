def _cast_values(parent, child, key):
    if not parent:
        pv = None
    else:
        pv = parent[key]

    v = child.relative_props.get(key)
    return pv, v


class Props:
    def __init__(self, dynamic_accumulators=None, dynamic_keys=None,
                 static_accumulators=None, required_static_keys=None):

        self.relative_props = {}
        self.static_props = {}

        self.dynamic_accumulators = dynamic_accumulators or {}
        # use accumulator keys as default dynamic keys
        self.dynamic_keys = dynamic_keys or list(self.dynamic_accumulators.keys())

        self.static_accumulators = static_accumulators
        self.required_static_keys = required_static_keys or []

        self.parent = None
        self.children = []

    def update(self, props):
        self.relative_props.update(props)

    @staticmethod
    def default_static_accumulator(parent, child, key):
        pv, v = _cast_values(parent, child, key)
        return v if v is not None else pv

    def accumulate_static(self, recursive=False):
        self.static_props = {}

        parent_static_props = {} if not self.parent else self.parent.static_props

        keys = list(parent_static_props.keys()) + list(self.relative_props.keys()) + self.required_static_keys
        keys = set(keys)
        keys = [key for key in keys if key not in self.dynamic_keys]

        for key in keys:
            accumulator = self.static_accumulators.get(key, self.default_static_accumulator)
            self.static_props[key] = accumulator(self.parent, self, key)

        if recursive:
            for child in self.children:
                child.accumulate_static(recursive=recursive)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def __getitem__(self, item):
        if item in self.dynamic_keys:
            return self.dynamic_accumulators[item](self.parent, self, item)
        else:
            return self.static_props.get(item)




if __name__ == '__main__':

    def t_acc(parent, child, key):
        pv, v = _cast_values(parent, child, key)
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


    def gen_props():
        return Props(dynamic_accumulators=DYNAMIC_ACCUMULATORS,
                     static_accumulators=STATIC_ACCUMULATORS,
                     required_static_keys=['size', 'time_size'])


    root = gen_props()
    child = gen_props()
    root.add_child(child)

    root.update({'t': 1, 'size': 5})
    child.update({'t': 2})
    root.accumulate_static(recursive=True)

    print(root.static_props, child.static_props, root['t'], child['t'])