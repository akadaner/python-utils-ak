from utils_ak.block_tree.pushers import stack_push
from utils_ak.block_tree.int_parallelepiped_block import IntParallelepipedBlock


class BlockMaker:
    def __init__(self, root_obj='root', default_push_func=stack_push, block_factory=None, **props):
        block_factory = block_factory or IntParallelepipedBlock

        if isinstance(root_obj, str):
            self.root = block_factory(root_obj, **props)
        elif isinstance(root_obj, IntParallelepipedBlock):
            assert len(props) == 0  # not supported case
            self.root = root_obj
        else:
            raise Exception('Unknown root type')

        self.blocks = [self.root]
        self.default_push_func = default_push_func

    def make(self, block_obj=None, push_func=None, push_kwargs=None, **kwargs):
        push_func = push_func or self.default_push_func
        push_kwargs = push_kwargs or {}

        if isinstance(block_obj, str) or block_obj is None:
            block = IntParallelepipedBlock(block_obj, **kwargs)
        elif isinstance(block_obj, IntParallelepipedBlock):
            block = block_obj
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
    return block_maker.root, block_maker.make


if __name__ == '__main__':
    print('Test 1')
    root, make = init_block_maker('root', axis=0)
    make('a', size=[1, 0])
    make('b', size=[5, 0])
    make('c', size=[2, 0])
    print(root)

    print('Test 2')
    root, make = init_block_maker('root', axis=1)
    with make('a1', size=[0, 3]):
        with make('b1', size=[5, 0]):
            make('c1', size=[2, 0])
    with make('a2', size=[0, 2]):
        make('b2')

    print(root)

