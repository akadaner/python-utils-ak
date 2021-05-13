import copy

from utils_ak.numeric import *
from utils_ak.simple_vector import *


from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block
from utils_ak.block_tree.validation import validate_disjoint_by_axis
from utils_ak.block_tree.pushers.pushers import simple_push

from loguru import logger


def dummy_push(
    parent, block, validator, max_tries=24, start_from="last_end", iter_props=None
):
    axis = parent.props["axis"]

    if is_int_like(start_from):
        cur_start = int(float(start_from))
    elif start_from == "beg":
        cur_start = block.props["x_rel"][axis]
    else:
        if not parent.children:
            cur_start = 0
        else:
            if start_from == "last_beg":
                cur_start = max(
                    [child.props["x_rel"][axis] for child in parent.children]
                )
            elif start_from == "last_end":
                cur_start = max(
                    [
                        (child.props["x_rel"] + child.size)[axis]
                        for child in parent.children
                    ]
                )
            else:
                raise Exception("Unknown beg type")

    iter_props = iter_props or [{}]

    cur_x = cast_simple_vector(block.n_dims)

    cur_try = 0

    # logger.debug('Inserting blocks', parent=parent.props['cls'], block=block.props['cls'])

    while cur_try < max_tries:
        dispositions = []
        for props in iter_props:
            # try to push
            props = copy.deepcopy(props)
            cur_x[axis] = cur_start
            props["x"] = cur_x
            # logger.debug('Trying to push from ', cur_start=cur_start)
            res = simple_push(parent, block, validator=validator, new_props=props)

            if isinstance(res, Block):
                # success
                # logger.debug('Success')
                return block
            else:
                assert isinstance(res, dict)

                if "disposition" in res:
                    dispositions.append(res["disposition"])

        disposition = min(dispositions) if len(dispositions) == len(iter_props) else 1
        # logger.debug('Disposition', disposition=disposition)
        cur_start += disposition
    raise Exception("Failed to push element")


def test_dummy_push():
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout()

    def brute_validator(parent, block):
        for c in parent.children[:-1]:
            validate_disjoint_by_axis(c, block, axis=parent.props["axis"])

    logger.debug("Dummy push test")
    root = ParallelepipedBlock("root", n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock("a", n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock("b", n_dims=1, size=[3], axis=0)
    dummy_push(root, a, brute_validator)
    dummy_push(root, b, brute_validator, start_from=0)
    logger.debug("Root", root=root)


if __name__ == "__main__":
    test_dummy_push()
