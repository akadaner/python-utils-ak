import itertools
from utils_ak.block_tree import *
from utils_ak.reflexion import *
from utils_ak.iteration import *


def optimize(maker, value_function, *args, **kwargs):
    maker_kwargs = get_default_args(maker)
    maker_kwargs = {k: v for k, v in maker_kwargs.items() if isinstance(v, tuple)}
    permutations = {
        k: list(set(itertools.permutations(v, len(v)))) for k, v in maker_kwargs.items()
    }
    values = []

    for props in iter_props(permutations):
        _kwargs = {}
        _kwargs.update(kwargs)
        _kwargs.update(props)
        try:
            values.append([props, maker(*args, **_kwargs)])
        except AssertionError:
            pass

    if len(values) == 0:
        raise Exception("No suitable solution found")

    df = pd.DataFrame(values, columns=["props", "output"])
    df["value"] = df["output"].apply(value_function)
    df = df.sort_values(by="value")
    return df


def f(order=(0, 1, 1)):
    return sum(order)


if __name__ == "__main__":
    print(optimize(f, lambda x: x))
