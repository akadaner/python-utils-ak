import numpy as np


def is_int(obj):
    return np.issubdtype(type(obj), np.integer)