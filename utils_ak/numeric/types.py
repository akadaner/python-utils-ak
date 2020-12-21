import numpy as np

def is_float(obj):
    return np.issubdtype(type(obj), np.float)


def is_int(obj):
    return np.issubdtype(type(obj), np.integer)


def is_int_like(obj):
    if isinstance(obj, str) or is_float(obj):
        obj = float(obj)
        return int(obj) == obj
    elif is_int(obj):
        return True
    else:
        return False


if __name__ == '__main__':
    print(is_int_like('1.0'))
    print(is_int_like('1.2'))