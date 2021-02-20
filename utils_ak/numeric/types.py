import numpy as np


def is_float(obj):
    return np.issubdtype(type(obj), np.float)


def is_int(obj):
    return np.issubdtype(type(obj), np.integer)


def is_numeric(obj):
    return is_int(obj) or is_float(obj)


def is_int_like(obj):
    if isinstance(obj, str) or is_float(obj):
        try:
            obj = float(obj)
            return int(obj) == obj
        except:
            return False
    elif is_int(obj):
        return True
    else:
        return False


def test_is_int_like():
    print(is_int_like(1))
    print(is_int_like("1"))
    print(is_int_like("1.0"))
    print(is_int_like("1.2"))
    print(is_int_like("1.2;;"))
