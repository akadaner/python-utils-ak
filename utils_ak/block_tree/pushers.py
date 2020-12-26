import numpy as np
from app.schedule_maker.utils.interval import calc_interval_length, cast_interval
from utils_ak.serialization import cast_js
from utils_ak import cast_dict_or_list


def validate_disjoint_by_axis(b1, b2, axis=0):
    try:
        disposition = int(b1.y[axis] - b2.x[axis])
    except:
        disposition = 1

    i1 = cast_interval(b1.x[axis], b2.y[axis])
    i2 = cast_interval(b2.x[axis], b2.y[axis])

    assert calc_interval_length(i1 & i2) == 0, cast_js({'disposition': disposition})

# def gen_pair_validator(validate):
#     def f(parent, new_block):
#         for b in parent.children:
#             validate(b, new_block)
#     return f


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


def stack_push(parent, block, axis=0):
    cur_end = 0 if not parent.children else max(c.y[axis] - parent.x[axis] for c in parent.children)
    block.props.relative_props.setdefault('x', np.zeros(block.n_dims))[axis] = cur_end
    return add_push(parent, block)

# # simple greedy algorithm
# def dummy_push(parent, block, max_tries=24, beg='last_end', end=PERIODS_PER_DAY * 10, axis='x', validator='x', iter_props=None):
#     # note: make sure parent abs props are updated
#     beg_key = 't' if axis == 'x' else 'y'
#
#     if beg == 'last_beg':
#         cur_beg = max([parent.beg(axis)] + [child.beg(axis) for child in parent.children])
#     elif beg == 'last_end':
#         cur_beg = max([parent.beg(axis)] + [child.end(axis) for child in parent.children])
#     elif isinstance(beg, int):
#         cur_beg = beg
#     else:
#         raise Exception('Unknown beg type')
#
#     # go to relative coordinates
#     cur_beg -= parent.beg(axis)
#
#     # print('Starting from', cur_beg, parent.props['class'], block.props['class'])
#     # print([parent.beg] + [(child.beg, child.end, child.length, child.props['size']) for child in parent.children])
#     # print([(child.props['class'], child.end, child.props.relative_props, child.beg) for child in parent.children])
#     end = min(end, cur_beg + max_tries)
#
#     iter_props = iter_props or [{}]
#
#     while cur_beg < end:
#         dispositions = []
#         for props in iter_props:
#             props = copy.deepcopy(props)
#             props[beg_key] = cur_beg
#             res = simple_push(parent, block, validator=validator, new_props=props)
#             if isinstance(res, Block):
#                 return block
#             elif isinstance(res, dict):
#                 # optimization by disposition from validate_disjoint error
#                 if 'disposition' in res:
#                     dispositions.append(res['disposition'])
#
#         if len(dispositions) == len(iter_props):
#             # all iter_props failed because of bad disposition
#             cur_beg += min(dispositions)
#         else:
#             cur_beg += 1
#
#         logging.info(['All dispositions', dispositions])
#     raise Exception('Failed to push element')

if __name__ == '__main__':
    from utils_ak.block_tree import IntParallelepipedBlock

    a = IntParallelepipedBlock('a', n_dims=2, x=np.array([1, 0]), size=[4, 0])
    b = IntParallelepipedBlock('b', n_dims=2, x=np.array([3, 0]), size=[3, 0])
    try:
        validate_disjoint_by_axis(a, b, 0)
    except AssertionError as e:
        print('AssertionError on disposition', e)

    root = IntParallelepipedBlock('root', n_dims=1, x=np.array([2]))
    a = IntParallelepipedBlock('a', n_dims=1, size=[4])
    b = IntParallelepipedBlock('b', n_dims=1, size=[3])
    stack_push(root, a)
    stack_push(root, b)
    print(root)

    #
    #
    # c = IntParallelepipedBlock('c', n_dims=2, x=np.array([3, 4]), size=np.array([1, 5]))
    # a.add_child(b)
    # b.add_child(c)
    #
    # print(a)
    # print(b)
    # print(c)
    # print(a.x, a.size, a.y)
    # print(b.x, b.size, b.y)
    # print(c.x, c.size, c.y)
    #
    # print()
    # print(a['b']['c'])