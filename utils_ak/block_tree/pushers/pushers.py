from loguru import logger

from utils_ak.coder import cast_dict_or_list
from utils_ak.simple_vector import *
from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock


def stack_push(parent, block):
    axis = parent.props["axis"]
    cur_end = (
        0
        if not parent.children
        else max(c.y[axis] - parent.x[axis] for c in parent.children)
    )

    x = block.props.get("x", cast_simple_vector(block.n_dims))

    assert not x[axis], "Cannot push non-zero axis"

    x[axis] = cur_end
    block.props.update(x=x)
    return add_push(parent, block)


def test_stack_push():
    logger.debug("Stack push test")
    root = ParallelepipedBlock("root", n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock("a", n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock("b", n_dims=1, size=[3], axis=0)
    stack_push(root, a)
    stack_push(root, b)
    logger.debug(root)


def simple_push(parent, block, validator=None, new_props=None):
    # update props for current try
    new_props = new_props or {}
    block.props.update(**new_props)

    parent.add_child(block)

    if validator:
        try:
            validator(parent, block)
        except AssertionError as e:
            try:
                # reset parent
                parent.remove_child(block)
                # extract assertion message json
                res = cast_dict_or_list(e.__str__())  # {'disposition': 2}
                res = res or {}
                return res
            except:
                return {}
    return block


def add_push(parent, block, new_props=None):
    return simple_push(parent, block, validator=None, new_props=new_props)


def push(parent, block, push_func=stack_push, **kwargs):
    return push_func(parent, block, **kwargs)


if __name__ == "__main__":
    test_stack_push()
