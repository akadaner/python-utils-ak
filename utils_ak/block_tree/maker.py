from utils_ak.block_tree.pushers import *
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock


class BlockMaker:
    def __init__(self, root_obj='root', default_push_func=stack_push, block_factory=None,  **props):
        self.block_factory = block_factory or ParallelepipedBlock

        if isinstance(root_obj, str):
            self.root = self.block_factory(root_obj, **props)
        elif isinstance(root_obj, ParallelepipedBlock):
            assert len(props) == 0  # not supported case
            self.root = root_obj
        else:
            raise Exception('Unknown root type')

        self.blocks = [self.root]
        self.default_push_func = default_push_func

    def create_block(self, block_obj, **kwargs):
        return self.block_factory(block_obj, **kwargs)

    def copy(self, block, with_children=True, with_props=False, prop_keys=None):
        res = self.create_block(block.props['class'], **block.props.relative_props)

        if with_children:
            for child in block.children:
                res.add_child(self.copy(child))

        if with_props:
            props = block.props.get_all_props()
            prop_keys = prop_keys or []
            if prop_keys:
                props = {k: v for k, v in props if k in prop_keys}
            props = {k: v for k, v in props.items() if v is not None}
            res.props.update(props)
        return res

    def make(self, block_obj=None, push_func=None, push_kwargs=None, **kwargs):
        push_func = push_func or self.default_push_func
        push_kwargs = push_kwargs or {}

        if isinstance(block_obj, str) or block_obj is None:
            block = self.create_block(block_obj, **kwargs)
        elif isinstance(block_obj, ParallelepipedBlock):
            block = block_obj
            block.props.update(kwargs)
        else:
            raise Exception('Unknown block obj type')

        push_func(self.blocks[-1], block, **push_kwargs)
        return BlockMakerContext(self, block)


class BlockMakerContext:
    def __init__(self, maker, block):
        self.maker = maker
        self.block = block

    def __enter__(self):
        self.maker.blocks.append(self.block)
        return self.block

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.maker.blocks.pop()


def init_block_maker(root_obj, default_push_func=stack_push, **kwargs):
    block_maker = BlockMaker(root_obj, default_push_func, **kwargs)
    return block_maker, block_maker.make

def test_block_maker1():
    maker, make = init_block_maker('root', axis=0)
    make('a', size=[1, 0])
    make('b', size=[5, 0])
    make(maker.create_block('c', size=[2, 0]), test=5)
    print(maker.root)
    print(maker.root['c'].props.get_all_props())


def test_block_maker2():
    maker, make = init_block_maker('root', axis=1)
    with make('a1', size=[0, 3]):
        with make('b1', size=[5, 0]):
            make('c1', size=[2, 0])
    with make('a2', size=[0, 2]):
        make('b2')

    print(maker.root)


def test_copy():
    maker, make = init_block_maker('root')
    with make('a1', x=[1,1], size=[1, 1], push_func=add_push):
        make('b1', size=[1, 1])
        with make('b2', size=[1, 1]):
            make('c1', size=[1, 1])
    print(maker.root)
    print(maker.copy(maker.root['a1']['b2']))
    print(maker.copy(maker.root['a1']['b2'], with_props=True))


if __name__ == '__main__':
    test_block_maker1()
    test_block_maker2()
    test_copy()