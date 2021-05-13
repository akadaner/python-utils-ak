import pandas as pd


def cast_prop_values(parent, child, key):
    pv = None if not parent else parent[key]
    v = child.relative_props.get(key)
    return pv, v


def relative_acc(parent, child, key, default=None, formatter=None):
    if key in child.relative_props:
        res = child.relative_props[key]
    else:
        if callable(default):
            default = default()
        res = default

    if formatter:
        res = formatter(res)
    return res


def cumsum_acc(parent, child, key, default=None, formatter=None):
    res = relative_acc(parent, child, key, default=default, formatter=formatter)
    if parent:
        res += parent[key]
    return res


class DynamicProps:
    def __init__(
        self, props=None, formatters=None, accumulators=None, required_keys=None
    ):
        self.accumulators = accumulators or {}
        self.required_keys = required_keys or []
        self.formatters = formatters or {}

        self.relative_props = self._format_props(props or {})

        self.parent = None
        self.children = []

        self.cache = {}

    def _format_props(self, props):
        props = dict(props)
        for k, fmt in self.formatters.items():
            if k in props:
                props[k] = fmt(k, props[k])
        return props

    @staticmethod
    def default_accumulator(parent, child, key):
        pv, v = cast_prop_values(parent, child, key)
        return v if v is not None else pv

    def update(self, **props):
        props = self._format_props(props)
        self.relative_props.update(**props)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def remove_child(self, child):
        self.children.remove(child)
        child.parent = None

    def __getitem__(self, item):
        accumulator = self.accumulators.get(item, self.default_accumulator)
        return accumulator(self.parent, self, item)

    def get(self, item, default=None):
        res = self[item]
        if res is None:
            res = default
        return res

    def keys(self):
        parent_keys = [] if not self.parent else self.parent.keys()
        res = (
            parent_keys
            + list(self.accumulators.keys())
            + list(self.relative_props.keys())
            + self.required_keys
        )
        return list(set(res))

    def all(self):
        return {key: self[key] for key in self.keys()}


def test_dynamic_props():
    ACCUMULATORS = {"<cumsum>": cumsum_acc, "<relative>": relative_acc}

    def gen_props(props=None):
        return DynamicProps(
            props=props,
            accumulators=ACCUMULATORS,
            required_keys=["<cumsum>", "<relative>", "<other>"],
        )

    root = gen_props({"<cumsum>": 1, "<relative>": 5, "<other>": 1})
    child1 = gen_props({"<cumsum>": 2})
    child2 = gen_props({"<cumsum>": 3})
    root.add_child(child1)
    child1.add_child(child2)

    values = []
    for i, node in enumerate([root, child1, child2]):
        values.append(
            [
                node[key]
                for key in ["<cumsum>", "<relative>", "<other>", "<non-existent_key>"]
            ]
        )
    print(
        pd.DataFrame(
            values, columns=["<cumsum>", "<relative>", "<other>", "<non-existent_key>"]
        )
    )

    for node in [root, child1, child2]:
        print(node.keys(), node.all())


if __name__ == "__main__":
    test_dynamic_props()
