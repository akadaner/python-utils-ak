from utils_ak.block_tree.pushers import *
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.code_block import *


class BlockMaker:
    def __init__(
        self,
        root_obj="root",
        default_push_func=stack_push,
        block_factory=None,
        default_row_width=0,
        default_col_width=0,
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
        self.default_col_width = default_col_width

    def create_block(self, block_obj, **kwargs):
        return self.block_factory(block_obj, **kwargs)

    def copy(self, block, with_children=True, with_props=False, prop_keys=None, size=None):
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

    def block(
        self,
        block_obj=None,
        push_func=None,
        push_kwargs=None,
        inplace=True,
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
        return self.block(*args, **kwargs)

    def row(self, *args, **kwargs):
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

        return self.block(*args, **kwargs)

    def col(self, *args, **kwargs):
        # - Size

        key = "size"
        if key in kwargs:
            if isinstance(kwargs[key], (list, tuple, SimpleVector)):
                assert kwargs[key][1] == self.default_row_width
            else:
                kwargs[key] = (self.default_row_width, kwargs[key])

        # - X

        key = "x"
        if key in kwargs:
            if isinstance(kwargs[key], (list, tuple, SimpleVector)):
                assert kwargs[key][1] == 0
            else:
                kwargs[key] = (0, kwargs[key])

        # - Return block

        return self.block(*args, **kwargs)


class BlockMakerContext:
    def __init__(self, maker, block):
        self.maker = maker
        self.block = block

    def __enter__(self):
        self.maker.blocks.append(self.block)
        return self.block

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.maker.blocks.pop()


# todo later: del [@marklidenberg]
def init_block_maker(root_obj, default_push_func=stack_push, **kwargs):
    block_maker = BlockMaker(root_obj, default_push_func, **kwargs)
    return block_maker, block_maker.block


def test_block_maker1():
    maker, make = init_block_maker("root", axis=0)
    make("a", size=[1, 0])
    make("b", size=[5, 0])
    make(maker.create_block("c", size=[2, 0]), test=5)
    print(maker.root)
    print(maker.root["c"].props.all())


def test_block_maker2():
    maker, make = init_block_maker("root", axis=1)
    with make("a1", size=[0, 3]):
        with make("b1", size=[5, 0]):
            make("c1", size=[2, 0])
    with make("a2", size=[0, 2]):
        make("b2")

    print(maker.root)


def test_copy():
    maker, make = init_block_maker("root")
    with make("a1", x=[1, 1], size=[1, 1], push_func=add_push):
        make("b1", size=[1, 1])
        with make("b2", size=[1, 1]):
            make("c1", size=[1, 1])
    print(maker.root)
    print(maker.copy(maker.root["a1"]["b2"]))
    print(maker.copy(maker.root["a1"]["b2"], with_props=True))


def test_shift_element():
    maker, make = init_block_maker("root", axis=1)
    with make("a1", size=[0, 3]):
        with make("b1", size=[5, 0]):
            make("c1", size=[2, 0])
    with make("a2", size=[0, 2]):
        make("b2")

    c1 = maker.root['a1']['b1']['c1']
    c1.props.update(x=[c1.props["x_rel"][0] + 2, c1.x[1]])

    print(maker.root)

if __name__ == "__main__":
    # test_block_maker1()
    # test_block_maker2()
    # test_copy()
    test_shift_element()