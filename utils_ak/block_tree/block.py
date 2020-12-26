from utils_ak.properties import DynamicProps



class Block:
    def __init__(self, block_class=None, default_block_class='block', **props):
        block_class = block_class or default_block_class
        props['class'] = block_class

        self.parent = None
        self.children = []

        self.props = DynamicProps(props=props, accumulators=ACCUMULATORS, required_keys=REQUIRED_KEYS)

    def __getitem__(self, item):
        if isinstance(item, str):
            res = [b for b in self.children if b.props['class'] == item]
        elif isinstance(item, int):
            res = self.children[item]
        elif isinstance(item, slice):
            # Get the start, stop, and step from the slice
            res = [self[ii] for ii in range(*item.indices(len(self)))]
        else:
            raise TypeError('Item type not supported')

        if not res:
            return

        if hasattr(res, '__len__') and len(res) == 1:
            return res[0]
        else:
            return res

    def length(self, axis='x'):
        return self.w if axis == 'x' else self.h

    @property
    def x1(self):
        return self.props['t']

    @property
    def x2(self):
        return self.x1 + self.length('x')

    @property
    def w(self):
        res = self.props['size']

        if res:
            return res
        else:
            # no length - get length from children!
            if not self.children:
                return 0
            else:
                # orient is the same as in the children
                return max([0] + [c.x2 - self.x1 for c in self.children])

    @property
    def y1(self):
        return self.props['y']

    @property
    def y2(self):
        return self.y1 + self.length('y')

    def beg(self, axis='x'):
        if axis == 'x':
            return self.x1
        else:
            return self.y1

    def end(self, axis='x'):
        if axis == 'x':
            return self.x2
        else:
            return self.y2

    @property
    def h(self):
        res = self.props.relative_props.get('h')

        if res:
            return res
        else:
            # no length - get length from children!
            if not self.children:
                # default h
                return 1
            else:
                # orient is the same as in the children
                return max([0] + [c.y2 - self.y1 for c in self.children])

    def interval(self, axis='x'):
        if axis == 'x':
            return cast_interval(self.x1, self.x2)
        elif axis == 'y':
            return cast_interval(self.y1, self.y2)

    @property
    def time_interval(self):
        return '[{}, {})'.format(cast_time(self.x1), cast_time(self.x2))

    def __str__(self):
        res = f'{self.props["class"]} ({self.y1}, {self.y2}] x ({self.x1}, {self.x2}]\n'

        for child in self.children:
            for line in str(child).split('\n'):
                if not line:
                    continue
                res += '  ' + line + '\n'
        return res

    def __repr__(self):
        return str(self)

    def iter(self):
        yield self

        for child in self.children:
            for b in child.iter():
                yield b

    def set_parent(self, parent):
        self.parent = parent
        self.props.parent = parent.props

    def add_child(self, block):
        self.children.append(block)
        self.props.children.append(block.props)

    def add(self, block):
        block.set_parent(self)
        self.add_child(block)
        return block


def simple_push(parent, block, validator='x', new_props=None):
    if validator in ['x', 'y']:
        axis = str(validator)
        validator = gen_pair_validator(validate=lambda b1, b2: validate_disjoint(b1, b2, axis=axis))

    # set parent for proper abs_props
    block.set_parent(parent)

    # update props for current try
    new_props = new_props or {}
    block.props.update(new_props)

    if validator:
        try:
            validator(parent, block)
        except AssertionError as e:
            try:
                # todo: hardcode
                return cast_dict_or_list(e.__str__()) # {'disposition': 2}
            except:
                return
    return parent.add(block)


def add_push(parent, block, new_props=None):
    return simple_push(parent, block, validator=None, new_props=new_props)


# simple greedy algorithm
def dummy_push(parent, block, max_tries=24, beg='last_end', end=PERIODS_PER_DAY * 10, axis='x', validator='x', iter_props=None):
    # note: make sure parent abs props are updated
    beg_key = 't' if axis == 'x' else 'y'

    if beg == 'last_beg':
        cur_beg = max([parent.beg(axis)] + [child.beg(axis) for child in parent.children])
    elif beg == 'last_end':
        cur_beg = max([parent.beg(axis)] + [child.end(axis) for child in parent.children])
    elif isinstance(beg, int):
        cur_beg = beg
    else:
        raise Exception('Unknown beg type')

    # go to relative coordinates
    cur_beg -= parent.beg(axis)

    # print('Starting from', cur_beg, parent.props['class'], block.props['class'])
    # print([parent.beg] + [(child.beg, child.end, child.length, child.props['size']) for child in parent.children])
    # print([(child.props['class'], child.end, child.props.relative_props, child.beg) for child in parent.children])
    end = min(end, cur_beg + max_tries)

    iter_props = iter_props or [{}]

    while cur_beg < end:
        dispositions = []
        for props in iter_props:
            props = copy.deepcopy(props)
            props[beg_key] = cur_beg
            res = simple_push(parent, block, validator=validator, new_props=props)
            if isinstance(res, Block):
                return block
            elif isinstance(res, dict):
                # optimization by disposition from validate_disjoint error
                if 'disposition' in res:
                    dispositions.append(res['disposition'])

        if len(dispositions) == len(iter_props):
            # all iter_props failed because of bad disposition
            cur_beg += min(dispositions)
        else:
            cur_beg += 1

        logging.info(['All dispositions', dispositions])
    raise Exception('Failed to push element')

def dummy_push_y(parent, block, **kwargs):
    return dummy_push(parent, block, axis='y', **kwargs)

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


if __name__ == '__main__':
    a = Block('a', t=5)
    b = Block('b', size=2)
    c = Block('c', size=1)

    dummy_push(a, b)
    dummy_push(a, c)
    print(a)
    print('No sizes specified', a.props['size'], a.props['time_size'], a.length(), a.x1, a.x2)
    print()

    maker = BlockMaker(default_push_func=dummy_push)
    make = maker.make

    with make('a'):
        make('b', size=2)
        make('c', size=1)

    print('Block maker')
    print(maker.root)

    maker = BlockMaker(default_push_func=add_push)
    make = maker.make

    with make('a', size=3, t=5):
        make('b', t=2, size=2)
        make('b', size=2)
        make('c', size=1)
    print('Add push')
    print(maker.root)

    print(maker.root['a']['b'][0].interval())
    print(maker.root['a'][2])

    b = Block('a', time_size=10)
    print(b.length())

    try:
        b = Block('a', time_size=11)
        print(b.props['time_size'])
        raise Exception('Should not happen')
    except AssertionError:
        print('Time size should be divided by 5')
