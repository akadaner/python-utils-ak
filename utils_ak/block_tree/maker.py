

class BlockMaker:
    def __init__(self, root='root', default_push_func=simple_push):
        if isinstance(root, str):
            self.root = Block(root)
        elif isinstance(root, Block):
            self.root = root
        else:
            raise Exception('Unknown root type')
        self.blocks = [self.root]
        self.default_push_func = default_push_func

    def make(self, block_obj=None, push_func=None, push_kwargs=None, **kwargs):
        push_func = push_func or self.default_push_func
        push_kwargs = push_kwargs or {}

        if isinstance(block_obj, str) or block_obj is None:
            block = Block(block_obj, **kwargs)
        elif isinstance(block_obj, Block):
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