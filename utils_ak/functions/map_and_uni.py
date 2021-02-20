from functools import wraps


def add_map(f):
    @wraps(f)
    def wrapper(value, *args, **kwds):
        if isinstance(value, list):
            return [f(x) for x in value]
        else:
            return f(value, *args, **kwds)

    return wrapper


def add_uni(f):
    @wraps(f)
    def wrapper(value, *args, **kwds):
        if isinstance(value, list):
            return f(value, *args, **kwds)
        else:
            return f([value], *args, **kwds)[0]

    return wrapper


if __name__ == "__main__":

    def f(v):
        return v ** 2

    print(add_map(f)(1))
    print(add_map(f)([1, 2, 3]))

    def g(l):
        return [x ** 2 for x in l]

    print(add_uni(g)(1))
    print(add_uni(g)([1, 2, 3]))
