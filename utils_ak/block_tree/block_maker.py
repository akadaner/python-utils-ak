from typing import Callable, Optional

from utils_ak.block_tree.pushers import *
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.code_block import *


class BlockMaker:
    """A helper to build blocks."""

    def __init__(
        self,
        root_obj: str = "root",
        default_push_func: Callable = stack_push,
        block_factory: Callable = None,
        default_row_width: int = 0,
        default_column_width: int = 0,
        **props,
    ):
        self.block_factory = block_factory or ParallelepipedBlock

        if isinstance(root_obj, str):
            self.root = self.block_factory(root_obj, **props)
        elif isinstance(root_obj, ParallelepipedBlock):
            assert len(props) == 0  # not supported case
            self.root = root_obj
        else:
            raise Exception("Unknown root type")

        self.blocks = [self.root]
        self.default_push_func = default_push_func

        self.default_row_width = default_row_width
        self.default_column_width = default_column_width

    def create_block(self, block_obj, **kwargs):
        return self.block_factory(block_obj, **kwargs)

    def copy(
        self,
        block: Block,
        with_children: bool = True,
        with_props: bool = False,
        prop_keys: list = None,
        size: list = [],
    ):
        res = self.create_block(block.props["cls"], **block.props.relative_props)

        if with_children:
            for child in block.children:
                res.add_child(self.copy(child))

        if with_props:
            props = block.props.all()
            prop_keys = prop_keys or []
            if prop_keys:
                props = {k: v for k, v in props if k in prop_keys}
            props = {k: v for k, v in props.items() if v is not None}
            res.props.update(**props)

        if size:
            # - Convert to mutable

            size = list(size)

            # - Replace with defaults

            if size[0] is None:
                size[0] = res.props["size"][0]

            if size[1] is None:
                size[1] = res.props["size"][1]

            # - Update size

            res.update_size(size)
        return res

    def push(
        self,
        block_obj: Optional[Block] = None,
        push_func: Callable = None,
        push_kwargs: dict = None,
        inplace: bool = True,
        **kwargs,
    ):
        push_func = push_func or self.default_push_func
        push_kwargs = push_kwargs or {}

        if isinstance(block_obj, str) or block_obj is None:
            block = self.create_block(block_obj, **kwargs)
        elif isinstance(block_obj, ParallelepipedBlock):
            block = block_obj

            if not inplace:
                block = self.copy(block)
            block.props.update(**kwargs)
        else:
            raise Exception("Unknown block obj type")

        push_func(self.blocks[-1], block, **push_kwargs)
        return BlockMakerContext(self, block)

    # todo later: del [@marklidenberg]
    def make(self, *args, **kwargs):
        return self.push(*args, **kwargs)

    def push_row(self, *args, **kwargs):
        """Block wrapper for adding x-axis blocks

        m.row(... x=3, size=5) <=> m.block(... x=(3, 0), size=[5, self.default_row_width])

        """

        # - Size

        key = "size"
        if key in kwargs:
            if isinstance(kwargs[key], (list, tuple, SimpleVector)):
                assert kwargs[key][1] == self.default_row_width
            else:
                kwargs[key] = (kwargs[key], self.default_row_width)

        # - X

        key = "x"
        if key in kwargs:
            if not isinstance(kwargs[key], (list, tuple, SimpleVector)):
                kwargs[key] = (kwargs[key], 0)

        # - Return block

        return self.push(*args, **kwargs)

    def push_column(
        self,
        *args,
        **kwargs,
    ):
        # - Size

        key = "size"
        if key in kwargs:
            if isinstance(kwargs[key], (list, tuple, SimpleVector)):
                assert kwargs[key][1] == self.default_column_width
            else:
                kwargs[key] = (self.default_column_width, kwargs[key])

        # - X

        key = "x"
        if key in kwargs:
            if isinstance(kwargs[key], (list, tuple, SimpleVector)):
                assert kwargs[key][1] == 0
            else:
                kwargs[key] = (0, kwargs[key])

        # - Return block

        return self.push(*args, **kwargs)


class BlockMakerContext:
    def __init__(self, m, block):
        self.m = m
        self.block = block

    def __enter__(self):
        self.m.blocks.append(self.block)
        return self.block

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.m.blocks.pop()


# todo later: del [@marklidenberg]
def init_block_maker(root_obj, default_push_func=stack_push, **kwargs):
    block_maker = BlockMaker(root_obj, default_push_func, **kwargs)
    return block_maker, block_maker.push


def test_block_m1():
    m = BlockMaker("root", axis=0, foo="bar")
    m.push("a", size=[1, 0])
    m.push("b", size=[5, 0])
    m.push(m.create_block("c", size=[2, 0]), test=5)
    print(m.root.props.all())
    print(m.root["c"].props.all())

    # - Getting child elements mechanics

    print(m.root["c"])
    print(m.root["c", True])
    m.push(m.create_block("c", size=[2, 0]), test=5)
    print(m.root["c"])
    print(m.root["c", True])

    # - Iterate mechanics

    for child in m.root.iter(cls="c"):
        print(child)

    for child in m.root.iter(cls=lambda cls: cls == "c"):
        print(child)


def test_block_m2():
    m = BlockMaker("root", axis=1)

    with m.push("a1", size=[0, 3]):
        with m.push("b1", size=[5, 0]):
            m.push("c1", size=[2, 0])
    with m.push("a2", size=[0, 2]):
        m.push("b2")

    print(m.root)


def test_copy():
    m = BlockMaker("root")
    with m.push("a1", x=[1, 1], size=[1, 1], push_func=add_push):
        m.push("b1", size=[1, 1])
        with m.push("b2", size=[1, 1]):
            m.push("c1", size=[1, 1])
    print(m.root)
    print(m.copy(m.root["a1"]["b2"]))
    print(m.copy(m.root["a1"]["b2"], with_props=True))
    print(m.root["a1"]["b2"].props["x"], m.root["a1"]["b2"].props["x_rel"])


def test_shift_element():
    m = BlockMaker("root", axis=1)
    with m.push("a1", size=[0, 3]):
        with m.push("b1", size=[5, 0]):
            m.push("c1", size=[2, 0])
    with m.push("a2", size=[0, 2]):
        m.push("b2")

    c1 = m.root["a1"]["b1"]["c1"]
    c1.props.update(x=[c1.props["x_rel"][0] + 2, c1.x[1]])

    print(m.root)


if __name__ == "__main__":
    test_block_m1()
    test_block_m2()
    test_copy()
    test_shift_element()
