import numpy as np

ERROR = 1e-5


def nanmin(arr, require_any=True):
    arr = [v if v is not None else np.nan for v in arr]
    if require_any and all(np.isnan(v) for v in arr):
        raise Exception("No values found")
    return float(np.nanmin(arr))
