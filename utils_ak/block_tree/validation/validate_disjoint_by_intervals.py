from utils_ak.block_tree import ParallelepipedBlock
from utils_ak.coder import cast_js
from utils_ak.lazy_tester import lazy_tester


def _calc_interval_intersection(i1, i2):
    return max(min(i1[1], i2[1]) - max(i1[0], i2[0]), 0)


def validate_disjoint_by_intervals(i1, i2, distance=0, ordered=False):
    """
    :param i1:
    :param i2:
    :param distance:
    :param ordered: i1 <= i2
    :return:
    """

    # add neighborhood to the first interval
    i1 = (i1[0] - distance, i1[1] + distance)

    if i1[0] >= i1[1]:
        return

    if _calc_interval_intersection(i1, i2) != 0 or (ordered and i2[0] <= i1[0]):
        try:
            disposition = int(i1[1] - i2[0])
        except:
            disposition = 1

        raise AssertionError(cast_js({"disposition": disposition}))


def test_interval_intersection():
    print(_calc_interval_intersection([0, 2], [3, 4]))
    print(_calc_interval_intersection([0, 2], [1, 3]))
