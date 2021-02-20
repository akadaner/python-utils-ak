from copy import deepcopy


def shatter_range(function, split, params, *args, **kwargs):
    from_name, to_name = split

    if len(params) == 2:
        from_, to_ = params
        spacing = 1
    else:
        from_, to_, spacing = params

    results = []

    for left in range(from_, to_, spacing):
        right = min(to_, left + spacing)

        kwargs.update({from_name: left, to_name: right})

        new_function = partial(function, *args, **kwargs)
        new_function.__setattr__(from_name, left)
        new_function.__setattr__(to_name, right)

        results.append(new_function)

    return results


def partial(function, *args, **kwargs):
    def partially_applied_function(*args2, **kwargs2):
        kwargs2.update(kwargs)
        return function(*(args + args2), **kwargs2)

    return partially_applied_function


def repeat(f, obj, n):
    res = obj
    for i in range(n):
        res = f(res)
    return res


if __name__ == "__main__":

    def go(left=1, right=100):
        return left, right

    ranges = shatter_range(go, split=["left", "right"], params=(1, 100, 5))
    assert ranges[1]() == (6, 11)

    go_from_5 = partial(go, left=5)
    assert go_from_5(right=10) == (5, 10)

    go_from_5 = partial(go, 5)
    assert go_from_5(10) == (5, 10)
#
