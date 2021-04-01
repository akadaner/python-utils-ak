""" Miscellaneous functionality with basic python built-in objects. """
import collections
import numpy
import itertools


def delistify(lst_obj):
    if isinstance(lst_obj, list):
        return lst_obj if (len(lst_obj) > 1 or not lst_obj) else lst_obj[0]
    else:
        return lst_obj


def delistify_single(iterable):
    lst = list(iterable)
    assert len(lst) == 1
    return lst[0]


def cast_list(lst):
    return lst if isinstance(lst, list) else [lst]


listify = cast_list


def iter_get(iterable, index=0, default=None):
    iterator = iter(iterable)

    if index >= 0:
        try:
            return next(itertools.islice(iterator, index, None))
        except StopIteration:
            return default
    else:
        lst = list(iterator)
        try:
            return lst[index]
        except IndexError:
            return default


def iter_len(iterable):
    return len(list(iterable))


def remove_duplicates(seq, key=None):
    # preserves order of sequence
    seen = set()
    seen_add = seen.add
    key = key or (lambda x: x)
    return [x for x in seq if not (key(x) in seen or seen_add(key(x)))]


def remove_neighbor_duplicates(seq, key=None):
    key = key or (lambda x: x)
    res = []
    for x in seq:
        if res and key(res[-1]) == key(x):
            continue
        res.append(x)
    return res


def apply_on_slice(f, lst, cond):
    ind = [i for i, v in enumerate(lst) if cond(v)]
    applied = f([lst[i] for i in ind])
    res = list(lst)
    for j, i in enumerate(ind):
        res[i] = applied[j]
    return res


# NOTE: use anyconfig.merge for more advanced merge logics
def update_dic(dic, new_dic):
    """
    :param dic: obj, dict by default
    :param new_dic: dict
    :return:
    """
    for key, val in new_dic.items():
        if isinstance(dic, collections.Mapping):
            if isinstance(val, collections.Mapping):
                dic[key] = update_dic(dic.get(key, {}), val)
            else:
                dic[key] = new_dic[key]
        else:
            # dic is not a dictionary. Make it one
            dic = {key: new_dic[key]}
    return dic


def crop_to_chunks(lst, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def split_list(lst, n):
    """Split list to n parts with equal size."""
    for array in numpy.array_split(lst, n):
        yield list(array)


def unfold_dic(dic, keys, get=False, default=None):
    if get:
        return [dic.get(key, default) for key in keys]
    else:
        return [dic[key] for key in keys]


def recycle_list(lst, idx):
    return (lst * 2)[idx % len(lst) :][: len(lst)]


def test_collection():
    d1 = {"k1": {"k3": 4}}
    d2 = {"k1": {"k3": [2, 3]}}
    print(update_dic(d1, d2), d1, d2)

    print(
        apply_on_slice(
            lambda lst: [x ** 2 for x in lst], [1, 2, 3, 4, 5, 6], lambda x: x % 2 == 0
        )
    )
    # print(filter_dic({'k': 1, 'e': 2}, leave=2))
    print(
        remove_duplicates(
            [
                1,
                2,
                3,
                4,
                5,
                1,
                1,
            ]
        )
    )
    print(
        remove_neighbor_duplicates(
            [
                1,
                2,
                3,
                4,
                5,
                1,
                1,
            ]
        )
    )

    lst = [1, 2, 3, 4]
    print(lst)
    print(recycle_list(lst, 2))


if __name__ == "__main__":
    test_collection()
