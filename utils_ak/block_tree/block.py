from utils_ak.properties import *
from utils_ak.architecture import delistify
from collections import defaultdict


class Block:
    def __init__(
        self,
        block_class=None,
        default_block_class="block",
        props_formatters=None,
        props_accumulators=None,
        props_required_keys=None,
        props_cache_keys=None,
        **props,
    ):
        block_class = block_class or default_block_class
        props["cls"] = block_class

        self.parent = None
        self.children = []
        self.children_by_cls = defaultdict(list)

        self.props = DynamicProps(
            props=props,
            formatters=props_formatters,
            accumulators=props_accumulators,
            required_keys=props_required_keys,
            cache_keys=props_cache_keys,
        )

    def __getitem__(self, item):
        res = self.get(item)
        if res is None:
            if isinstance(item, str):
                raise KeyError(item)
            elif isinstance(item, int):
                return IndexError(item)
            else:
                raise Exception(f"Not found: {item}")
        return res

    def children_classes(self):
        return [child.props["cls"] for child in self.children]

    def get(self, item):
        return_list = False
        if isinstance(item, tuple):
            item, return_list = item
        if isinstance(item, str):
            default_value = [] if return_list else None
            res = self.children_by_cls.get(item, default_value)
        elif isinstance(item, int):
            res = [self.children[item]]
        elif isinstance(item, slice):
            # Get the start, stop, and step from the slice
            res = [self[ii] for ii in range(*item.indices(len(self)))]
        else:
            raise TypeError("Item type not supported")

        if not return_list:
            res = delistify(res)
        return res

    def __str__(self):
        res = f'{self.props["cls"]}\n'

        for child in self.children:
            for line in str(child).split("\n"):
                if not line:
                    continue
                res += "  " + line + "\n"
        return res

    def __repr__(self):
        return str(self)

    def query_match(self, query):
        for k, v in query.items():
            if callable(v):
                if not v(self.props[k]):
                    return False
            else:
                if self.props[k] != v:
                    return False
        return True

    def iter(self, **query):
        if self.query_match(query):
            yield self

        for child in self.children:
            for b in child.iter(**query):
                yield b

    def find(self, **query):
        return list(self.iter(**query))

    def find_one(self, **query):
        for obj in self.iter(**query):
            # return first value found
            return obj
        raise Exception("Nothing found.")

    def add_child(self, block):
        block.parent = self
        self.children.append(block)
        self.children_by_cls[block.props["cls"]].append(block)
        self.props.add_child(block.props)
        return block

    def remove_child(self, block):
        block.parent = None
        self.children.remove(block)
        self.children_by_cls[block.props["cls"]].remove(block)
        self.props.remove_child(block.props)
        return block

    def detach_from_parent(self):
        if self.parent:
            self.parent.remove_child(self)


def test_block():
    def cast_block(block_class, **kwargs):
        return Block(
            block_class,
            props_accumulators={
                "t": lambda parent, child, key: cumsum_acc(
                    parent, child, key, default=0, formatter=int
                )
            },
            **kwargs,
        )

    a = cast_block("a", t=5)
    b = cast_block("b")
    c = cast_block("c", t=3)
    a.add_child(b)
    b.add_child(c)

    print(a)
    print(b)
    print(c)
    print(a.props["t"])
    print(b.props["t"])
    print(c.props["t"])

    print()
    print(a["b"]["c"])

    print("Test query")
    for b in a.iter(cls=c):
        print(b)


if __name__ == "__main__":
    test_block()
