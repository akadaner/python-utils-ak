import pandas as pd
import numpy as np


def is_none_like(v):
    if v is None:
        return

    try:
        if np.isnan(v):
            return True
    except:
        pass

    try:
        if pd.isnull(v):
            return True
    except:
        pass

    return False
