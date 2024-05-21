from functools import partial

from utils_ak.simple_vector import *
from utils_ak.properties import *
from utils_ak.block_tree import Block


def x_cumsum_acc(parent, child, key, default=None, formatter=None):
    if parent:
        return parent[key].add(relative_acc(parent, child, key, default=default, formatter=formatter))
    return SimpleVector(list(relative_acc(parent, child, key, default=default, formatter=formatter).values))


# todo later: put outside for pickle compatability [@marklidenberg]
def x_rel_acc(parent, child, key):
    return relative_acc(parent, child, "x")


def _init_simple_vector(k, v):
    return SimpleVector([int(x) for x in v])


class ParallelepipedBlock(Block):
    def __init__(self, block_class, n_dims=2, **props):
        self.n_dims = n_dims

        if "x" not in props:
            props["x"] = SimpleVector(n_dims)
        if "size" not in props:
            props["size"] = SimpleVector(n_dims)

        self.size_cached = None

        super().__init__(
            block_class,
            props_formatters={
                "x": _init_simple_vector,
                "size": _init_simple_vector,
            },
            props_cache_keys=["x"],
            **props,
        )

        self.props.accumulators["x"] = x_cumsum_acc
        self.props.accumulators["x_rel"] = x_rel_acc
        self.props.accumulators["size"] = relative_acc
        self.props.accumulators["axis"] = partial(relative_acc, default=0)

    @property
    def x(self):
        return self.props["x"]

    @property
    def x_rel(self):
        return self.props["x_rel"]

    @property
    def y(self):
        return self.x + self.size

    @property
    def y_rel(self):
        return self.x_rel + self.size

    def add_child(self, block):
        super().add_child(block)
        self.size_cached = None

    def remove_child(self, block):
        super().remove_child(block)
        self.size_cached = None

    @property
    def size(self):
        if not self.size_cached:
            size = self.props["size"]
            values = []
            for axis in range(self.n_dims):
                if size[axis] == 0:
                    if not self.children:
                        values.append(0)
                    else:
                        start = min([c.x_rel[axis] for c in self.children] + [0])
                        values.append(max([c.y_rel[axis] - start for c in self.children]))
                else:
                    values.append(size[axis])
            self.size_cached = SimpleVector(values)
        return self.size_cached

    def reset_cache(self, recursion="down"):
        self.size_cached = None

        if recursion == "down":
            for child in self.children:
                child.reset_cache(recursion="down")
        elif recursion == "up":
            if self.parent:
                self.parent.reset_cache(recursion="up")

    def update_size(self, size):
        self.props.update(size=size)
        self.reset_cache(recursion="up")

    def to_dict(self, props=None, with_children=True):
        res = {}
        res["cls"] = self.props["cls"]
        res["n_dims"] = self.n_dims

        if not props:
            rel_props = dict(self.props.relative_props)
            rel_props = {k: list(v) if isinstance(v, SimpleVector) else v for k, v in rel_props.items()}
            res["props"] = rel_props
        else:
            res["props"] = {}

            for prop in props:
                if isinstance(prop, str):
                    res["props"][prop] = self.props.get(prop)

                elif isinstance(prop, dict):
                    if "cls" not in prop or ("cls" in prop and self.props["cls"] == prop["cls"]):
                        key = prop["key"]
                        value = prop.get("value", key)
                        if isinstance(value, str):
                            res["props"][key] = self.props[key]
                        elif callable(value):
                            res["props"][key] = value(self)
                        else:
                            raise Exception("Value should be either callable or string")

        if with_children:
            res["children"] = [child.to_dict(props, with_children=True) for child in self.children]
        return res

    def __str__(self, props: list = []):
        res = self.props["cls"]

        if self.props["label"]:
            res += ": " + self.props["label"]

        def _format_coordinate(value, axis=0):
            # todo later: remove, hardcode: load cast_time from unagrande project
            try:
                from app.scheduler.common.time_utils import cast_time
            except ImportError:
                cast_time = lambda x: x

            if axis == 0 and value:
                return cast_time(value)
            elif axis != 0 and value:
                return value
            else:
                return "-"

        res += " " + " x ".join(
            [
                f"({_format_coordinate(self.x[i], axis=i)}, {_format_coordinate(self.y[i], axis=i)}]"
                for i in range(self.n_dims)
            ]
        )

        if props:
            res += " " + " ".join([f"{prop}: {self.props[prop]}" for prop in props])

        for child in self.children:
            for line in child.__str__(props=props).split("\n"):
                if not line:
                    continue
                res += "\n  " + line
        return res

    def __repr__(self):
        return str(self)

    def is_leaf(self):
        return not self.children

    def tabular(self):
        res = ""
        for b in self.iter():
            if b.size[0] != 0:
                res += " " * int(b.x[0]) + "=" * int(b.size[0]) + f' {b.props["cls"]} '
                if b.props["label"]:
                    res += f': {b.props["label"]} '
                res += " x ".join([f"({b.x[i]}, {b.y[i]}]" for i in range(b.n_dims)])
                res += "\n"
        return res

    @staticmethod
    def from_dict(dic):
        res = ParallelepipedBlock(dic["cls"], dic["n_dims"], **dic["props"])
        for child in dic["children"]:
            res.add_child(ParallelepipedBlock.from_dict(child))

        return res


def test_parallelepiped_block():
    a = ParallelepipedBlock("a", n_dims=2, x=[1, 2])
    b = ParallelepipedBlock("b", n_dims=2)
    c = ParallelepipedBlock("c", n_dims=2, x=[3, 4], size=[1, 5])
    print(a.props.cache, b.props.cache, c.props.cache)
    a.add_child(b)
    print(a.props.cache, b.props.cache, c.props.cache)
    b.add_child(c)
    print(a.props.cache, b.props.cache, c.props.cache)
    print(a)
    print(a.props.cache, b.props.cache, c.props.cache)
    print(b)
    print(a.props.cache, b.props.cache, c.props.cache)
    print(c)

    print()

    a_enc = a.to_dict()
    print(a_enc)
    print(ParallelepipedBlock.from_dict(a_enc))

    print(a.x, a.size, a.y)
    print(b.x, b.size, b.y)
    print(c.x, c.size, c.y)

    print()
    print(a["b"]["c"])

    print(a.__repr__())

    print(a.to_dict())
    print(a.to_dict(["x", {"key": "size", "value": lambda b: list(b.props["size"])}]))


if __name__ == "__main__":
    test_parallelepiped_block()
