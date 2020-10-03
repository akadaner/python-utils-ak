from functools import wraps


def one_to_many(f):
    @wraps(f)
    def wrapper(value, *args, **kwds):
        if isinstance(value, list):
            return [f(x) for x in value]
        else:
            return f(value, *args, **kwds)
    return wrapper


def many_to_one(f):
    @wraps(f)
    def wrapper(value, *args, **kwds):
        if isinstance(value, list):
            return f(value, *args, **kwds)
        else:
            return f([value], *args, **kwds)[0]
    return wrapper
