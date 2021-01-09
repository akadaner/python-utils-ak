import numpy as np


def calc_minimum_pressure(pressures):
    pressures = [p if p is not None else np.nan for p in pressures]
    if all(np.isnan(p) for p in pressures):
        raise Exception('No pressures specified')
    return float(np.nanmin(pressures))

