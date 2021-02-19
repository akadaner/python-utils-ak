import copy

from utils_ak.serialization import cast_dict_or_list
from utils_ak.numeric import *
from utils_ak.simple_vector import *

from utils_ak.block_tree.parallelepiped_block import ParallelepipedBlock
from utils_ak.block_tree.block import Block
from utils_ak.block_tree.validation import validate_disjoint_by_axis

from utils_ak.block_tree.pushers.pushers import *

from loguru import logger


class IterativePusher:
    def __init__(self):
        self.parent = None
        self.block = None

    def init(self):
        pass

    def update(self, results):
        pass

    def __call__(self, *args, **kwargs):
        return self.push(*args, **kwargs)

    def push(self, parent, block, validator, iter_props=None, max_tries=24):
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
        raise AssertionError('Failed to push element')


class AxisPusher(IterativePusher):
    def __init__(self, start_from='last_end'):
        super().__init__()
        self.start_from = start_from

    def init(self):
        self.axis = self.parent.props['axis']

        if is_int_like(self.start_from):
            cur_start = int(float(self.start_from))
        elif self.start_from == 'beg':
            cur_start = self.block.props['x_rel'][self.axis]
        else:
            if not self.parent.children:
                cur_start = 0
            else:
                if self.start_from == 'last_beg':
                    cur_start = max([child.props['x_rel'][self.axis] for child in self.parent.children])
                elif self.start_from == 'last_end':
                    cur_start = max([(child.props['x_rel'] + child.size)[self.axis] for child in self.parent.children])
                else:
                    raise Exception('Unknown beg type')
        self.cur_x = cast_simple_vector(self.block.n_dims)
        self.cur_x[self.axis] = cur_start
        self.block.props.update(x=self.cur_x)

    def update(self, results):
        dispositions = [result.get('disposition', None) for result in results]
        dispositions = [d for d in dispositions if d is not None]
        disposition = min(dispositions) if len(dispositions) == len(results) else 1
        logger.debug('Disposition', disposition=disposition)
        self.cur_x[self.axis] += disposition
        self.block.props.update(x=self.cur_x)


def test_axis_pusher():
    from utils_ak.loguru import configure_loguru_stdout
    configure_loguru_stdout()
    def brute_validator(parent, block):
        for c in parent.children:
            validate_disjoint_by_axis(c, block, axis=parent.props['axis'])

    logger.debug('Dummy push test')
    root = ParallelepipedBlock('root', n_dims=1, x=[2], axis=0)
    a = ParallelepipedBlock('a', n_dims=1, size=[4], axis=0)
    b = ParallelepipedBlock('b', n_dims=1, size=[3], axis=0)

    AxisPusher().push(root, a, brute_validator)
    AxisPusher(start_from=0).push(root, b, brute_validator)
    logger.debug('Root', root=root)


if __name__ == '__main__':
    test_axis_pusher()

