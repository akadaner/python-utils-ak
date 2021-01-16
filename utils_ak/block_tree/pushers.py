import logging
import copy

from utils_ak.clock import *
from utils_ak import cast_dict_or_list
from utils_ak.numeric import *
from utils_ak.simple_vector import *

from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block
from utils_ak.block_tree.validation import validate_disjoint_by_axis


def stack_push(parent, block):
    axis = parent.props['axis']
    cur_end = 0 if not parent.children else max(c.y[axis] - parent.x[axis] for c in parent.children)

    x = block.props.get('x', cast_simple_vector(block.n_dims))
    x[axis] = cur_end
    block.props.update(x=x)
    return add_push(parent, block)


def test_stack_push():
    print('Stack push test')
    root = ParallelepipedBlock('root', n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock('a', n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock('b', n_dims=1, size=[3], axis=0)
    stack_push(root, a)
    stack_push(root, b)
    print(root)

@clockify()
def simple_push(parent, block, validator=None, new_props=None):
    block.set_parent(parent)

    # update props for current try
    new_props = new_props or {}
    block.props.update(**new_props)
    if validator:
        try:
            validator(parent, block)
        except AssertionError as e:

            try:
                # reset parent
                block.parent = None

                # extract assertion message json
                return cast_dict_or_list(e.__str__())  # {'disposition': 2}
            except:
                return {}
    res = parent.add_child(block)
    return res


def add_push(parent, block, new_props=None):
    return simple_push(parent, block, validator=None, new_props=new_props)


def dummy_push(parent, block, validator, max_tries=24, start_from='last_end', iter_props=None):
    # print('Pushing', parent.props['class'], block.props['class'])
    clock('1')
    axis = parent.props['axis']

    if is_int(start_from):
        cur_start = start_from
    elif start_from == 'beg':
        cur_start = block.props['x_rel'][axis]
    else:
        if not parent.children:
            cur_start = 0
        else:
            if start_from == 'last_beg':
                cur_start = max([child.props['x_rel'][axis] for child in parent.children])
            elif start_from == 'last_end':
                cur_start = max([(child.props['x_rel'] + child.size)[axis] for child in parent.children])
            else:
                raise Exception('Unknown beg type')

    iter_props = iter_props or [{}]

    cur_x = cast_simple_vector(block.n_dims)

    cur_try = 0
    clock('1')

    while cur_try < max_tries:
        dispositions = []
        for props in iter_props:
            # try to push
            props = copy.deepcopy(props)
            cur_x[axis] = cur_start
            props['x'] = cur_x
            # print('Trying to push from ', cur_start)
            res = simple_push(parent, block, validator=validator, new_props=props)

            if isinstance(res, Block):
                # success
                return block
            else:
                assert isinstance(res, dict)

                if 'disposition' in res:
                    # print('Disposition', res)
                    dispositions.append(res['disposition'])
        # print('Dispositions', dispositions)
        if len(dispositions) == len(iter_props):
            # all iter_props failed because of bad disposition
            cur_start += min(dispositions)
        else:
            # other reason
            cur_start += 1
    raise Exception('Failed to push element')


def test_dummy_push():
    def brute_validator(parent, block):
        for c in parent.children:
            validate_disjoint_by_axis(c, block, axis=parent.props['axis'])

    print('Dummy push test')
    root = ParallelepipedBlock('root', n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock('a', n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock('b', n_dims=1, size=[3], axis=0)
    dummy_push(root, a, brute_validator)
    dummy_push(root, b, brute_validator, start_from=0)
    print(root)



def push(parent, block, push_func=stack_push, **kwargs):
    return push_func(parent, block, **kwargs)


if __name__ == '__main__':
    test_stack_push()
    test_dummy_push()