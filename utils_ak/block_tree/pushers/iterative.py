from utils_ak.imports.external import *

import copy

from utils_ak.coder import cast_dict_or_list
from utils_ak.numeric import *
from utils_ak.simple_vector import *

from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block
from utils_ak.block_tree.validation import validate_disjoint_by_axis

from utils_ak.block_tree.pushers.pushers import *


class IterativePusher:
    def __init__(self, validator=None):
        self.parent = None
        self.block = None
        self.validator = validator

    def init(self):
        pass

    def update(self, results):
        pass

    def __call__(self, *args, **kwargs):
        return self.push(*args, **kwargs)

    def push(self, parent, block, validator=None, iter_props=None, max_tries=300):
        validator = validator or self.validator
        self.parent = parent
        self.block = block

        iter_props = iter_props or [{}]

        self.init()

        cur_try = 0

        while cur_try < max_tries:
            results = []
            for props in iter_props:
                # try to push
                props = copy.deepcopy(props)

                res = simple_push(parent, block, validator=validator, new_props=props)

                if isinstance(res, Block):
                    # success
                    return block
                else:
                    assert isinstance(res, dict)
                    results.append(res)

            self.update(results)
            cur_try += 1

        logger.info(
            "Failed to push element",
            parent=parent.props["cls"],
            block=block.props["cls"],
        )
        raise AssertionError("Failed to push element")


class AxisPusher(IterativePusher):
    def __init__(
        self,
        start_from="max_end",
        start_shift=0,
        min_start=None,
        validator=None,
    ):
        super().__init__(validator=validator)
        self.start_from = start_from
        self.start_shift = start_shift
        self.min_start = min_start

    def _resolve_start_from(self, start_from):
        if isinstance(start_from, list):
            return max([self._resolve_start_from(x) for x in start_from])
        elif is_int_like(start_from):
            return int(float(start_from))
        elif start_from == "beg":
            return self.block.props["x_rel"][self.axis]
        else:
            if not self.parent.children:
                return 0
            else:
                if start_from == "max_beg":
                    return max([child.props["x_rel"][self.axis] for child in self.parent.children])
                if start_from == "last_beg":
                    return self.parent.children[-1].props["x_rel"][self.axis]
                elif start_from == "max_end":
                    return max([(child.props["x_rel"] + child.size)[self.axis] for child in self.parent.children])
                if start_from == "last_end":
                    return (self.parent.children[-1].props["x_rel"] + self.parent.children[-1].size)[self.axis]
                else:
                    raise Exception("Unknown beg type")

    def init(self):
        self.axis = self.parent.props["axis"]
        cur_start = self._resolve_start_from(self.start_from)
        cur_start += self.start_shift

        if self.min_start is not None:
            cur_start = max(cur_start, self.min_start)

        self.cur_x = cast_simple_vector(self.block.n_dims)
        self.cur_x[self.axis] = cur_start
        self.block.props.update(x=self.cur_x)

    def update(self, results):
        dispositions = [result.get("disposition", None) for result in results]
        dispositions = [d for d in dispositions if d is not None]
        disposition = min(dispositions) if len(dispositions) == len(results) else 1

        # logger.debug("Disposition", disposition=disposition)
        self.cur_x[self.axis] += disposition
        self.block.props.update(x=self.cur_x)


class ShiftPusher(IterativePusher):
    def __init__(self, period, start_from=None, validator=None):
        super().__init__(validator=validator)
        self.period = period
        self.start_from = start_from

    def _shift(self, period):
        self.block.props.update(x=[self.block.props["x_rel"][0] + period, self.block.x[1]])

    def init(self):
        if self.start_from:
            self.block.props.update(x=[self.start_from, self.block.x[1]])

    def update(self, results):
        self._shift(self.period)


def test_axis_pusher():
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout()

    def brute_validator(parent, block):
        for c in parent.children:
            validate_disjoint_by_axis(c, block, axis=parent.props["axis"])

    logger.debug("Dummy push test")
    root = ParallelepipedBlock("root", n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock("a", n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock("b", n_dims=1, size=[3], axis=0)

    AxisPusher().push(root, a, brute_validator)
    AxisPusher(start_from=0).push(root, b, brute_validator)
    logger.debug("Root", root=root)


if __name__ == "__main__":
    test_axis_pusher()
