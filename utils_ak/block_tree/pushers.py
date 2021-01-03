from utils_ak.serialization import cast_js
from utils_ak import cast_dict_or_list
from utils_ak.portion import *
from utils_ak.block_tree import Block
from utils_ak.numeric import *
from utils_ak.simple_vector import *
import logging
import copy


def simple_push(parent, block, validator=None, new_props=None):
    block.set_parent(parent)

    # update props for current try
    new_props = new_props or {}
    block.props.update(new_props)

    if validator:
        try:
            validator(parent, block)
        except AssertionError as e:
            try:
                block.parent = None
                # extract assertion message json
                return cast_dict_or_list(e.__str__())  # {'disposition': 2}
            except:
                return
    return parent.add_child(block)


def add_push(parent, block, new_props=None):
    return simple_push(parent, block, validator=None, new_props=new_props)


def stack_push(parent, block):
    axis = parent.props['axis']
    cur_end = 0 if not parent.children else max(c.y[axis] - parent.x[axis] for c in parent.children)
    block.props.relative_props.setdefault('x', cast_simple_vector(block.n_dims))[axis] = cur_end
    return add_push(parent, block)


def validate_disjoint_by_axis(b1, b2, axis=0):
    try:
        disposition = int(b1.y[axis] - b2.x[axis])
    except:
        disposition = 1

    i1 = cast_interval(b1.x[axis], b1.y[axis])
    i2 = cast_interval(b2.x[axis], b2.y[axis])

    assert calc_interval_length(i1 & i2) == 0, cast_js({'disposition': disposition})


def push(parent, block, push_func=stack_push, **kwargs):
    return push_func(parent, block, **kwargs)


def dummy_push(parent, block, validator, max_tries=24, start_from='last_end', end=288 * 10, iter_props=None):
    axis = parent.props['axis']

    # note: make sure parent abs props are updated
    if is_int(start_from):
        cur_start = start_from
    else:
        if not parent.children:
            cur_start = 0
        else:
            if start_from == 'last_beg':
                cur_start = max([child.props['x_rel'][axis] for child in parent.children])
            elif start_from == 'last_end':
                cur_start = max([(child.props['x_rel'] + child.size)[axis] for child in parent.children])
            elif isinstance(start_from, int):
                cur_start = start_from
            else:
                raise Exception('Unknown beg type')

    end = min(end, cur_start + max_tries)

    iter_props = iter_props or [{}]

    cur_x = cast_simple_vector(block.n_dims)

    while cur_x[axis] < end:
        dispositions = []
        for props in iter_props:
            props = copy.deepcopy(props)
            cur_x[axis] = cur_start
            props['x'] = cur_x
            res = simple_push(parent, block, validator=validator, new_props=props)

            if isinstance(res, Block):
                return block
            elif isinstance(res, dict):
                if 'disposition' in res:
                    dispositions.append(res['disposition'])

        if len(dispositions) == len(iter_props):
            # all iter_props failed because of bad disposition
            cur_start += min(dispositions)
        else:
            cur_start += 1
    raise Exception('Failed to push element')


if __name__ == '__main__':
    from utils_ak.block_tree import ParallelepipedBlock
    print('Stack push test')
    root = ParallelepipedBlock('root', n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock('a', n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock('b', n_dims=1, size=[3], axis=0)
    stack_push(root, a)
    stack_push(root, b)
    print(root)

    print('Validate disjoint test')
    for t in range(0, 10):
        a = ParallelepipedBlock('a', n_dims=1, x=[t], size=[4])
        b = ParallelepipedBlock('b', n_dims=1, x=[3], size=[3])
        print(a, b)
        try:
            validate_disjoint_by_axis(a, b, 0)
        except AssertionError as e:
            print('AssertionError on disposition', e)

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
