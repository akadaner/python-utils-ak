

from utils_ak.properties import *
from utils_ak.architecture import delistify


class Block:
    def __init__(self, block_class=None, default_block_class='block', props_accumulators=None, props_required_keys=None, **props):
        block_class = block_class or default_block_class
        props['class'] = block_class

        self.parent = None
        self.children = []

        self.props = DynamicProps(props=props, accumulators=props_accumulators, required_keys=props_required_keys)

    def __getitem__(self, item):
        res = self.get(item)
        if not res:
            if isinstance(res, str):
                raise KeyError(item)
            elif isinstance(res, int):
                return IndexError(item)
            else:
                raise Exception(f'Not found: {item}')
        return res

    def get(self, item):
        if isinstance(item, str):
            res = [b for b in self.children if b.props['class'] == item]
        elif isinstance(item, int):
            res = self.children[item]
        elif isinstance(item, slice):
            # Get the start, stop, and step from the slice
            res = [self[ii] for ii in range(*item.indices(len(self)))]
        else:
            raise TypeError('Item type not supported')
        return delistify(res)

    def __str__(self):
        res = f'{self.props["class"]}\n'

        for child in self.children:
            for line in str(child).split('\n'):
                if not line:
                    continue
                res += '  ' + line + '\n'
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

    def iter(self, query=None):
        query = query or {}

        if self.query_match(query):
            yield self

        for child in self.children:
            for b in child.iter(query):
                yield b

    def set_parent(self, parent):
        self.parent = parent
        self.props.parent = parent.props

    def add_child(self, block):
        block.set_parent(self)
        self.children.append(block)
        self.props.children.append(block.props)
        return block


if __name__ == '__main__':
    def cumsum_acc(parent, child, key, default=lambda: 0):
        pv, v = cast_prop_values(parent, child, key)
        pv = pv if pv else default()
        v = v if v else default()
        return pv + v

    def cast_block(block_class, **kwargs):
        return Block(block_class, props_accumulators={'t': cumsum_acc}, **kwargs)

    a = cast_block('a', t=5)
    b = cast_block('b')
    c = cast_block('c', t=3)
    a.add_child(b)
    b.add_child(c)

    print(a)
    print(b)
    print(c)
    print(a.props['t'])
    print(b.props['t'])
    print(c.props['t'])

    print()
    print(a['b']['c'])


    print('Test query')
    for b in a.iter({'class': 'c'}):
        print(b)