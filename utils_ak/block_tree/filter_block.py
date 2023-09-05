from typing import Callable

from utils_ak.block_tree import Block, x_cumsum_acc, init_block_maker


def filter_block(block, cond: Callable):
    if not block.children:
        if cond(block):
            return block
        else:
            return None
    else:
        block.children = [filter_block(b, cond=cond) for b in block.children]
        block.children = [b for b in block.children if b is not None]
        if not block.children:
            return None
        return block


def test():
    maker, make = init_block_maker("root", axis=0)
    make("a", size=[1, 0])
    make("b", size=[5, 0])
    make(maker.create_block("c", size=[2, 0]), test=5)

    print(filter_block(maker.root, lambda b: b.props['test'] == 5))


if __name__ == '__main__':
    test()