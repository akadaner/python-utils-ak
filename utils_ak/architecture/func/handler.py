""" Handler is a callable objects with multiple callbacks, filters and formatters. Supports asyncio run"""

from utils_ak.builtin import listify, delistify
import inspect


# todo: optimize aio code! Execute all callbacks using wait or something!


def is_aio(func):
    if inspect.iscoroutinefunction(func):
        return True
    elif hasattr(func, "is_aio") and getattr(func, "is_aio"):
        return True
    else:
        return False


class Handler(object):
    """Complex func wrapper. Computational graph with multiple filters, multiple formatters and multiple callbacks."""

    def __init__(self, callback=None, formatter=None, filter=None, reducer=None):
        """
        :param filter: callable or list of callable. Each filter should return True or False if rule is not specified (see Handler.add_filter for more)
        :param formatter: callable or list of callable. Each formatter should return args, kwargs tuple
        :param callback: callable or list of callable
        :return func return if func is single and list or returns of callbacks otherwise (a list)

        See __main__ for examples.
        """
        self.filters = []
        self.formatters = []
        self.callbacks = []

        self.add_filter(filter)
        self.add_formatter(formatter)
        self.add_callback(callback)
        self.reducer = reducer or delistify

    @property
    def is_aio(self):
        return any(is_aio(callback) for callback in self.callbacks)

    def add(self, callback=None, formatter=None, filter=None):
        self.add_formatter(formatter)
        self.add_filter(filter)
        self.add_callback(callback)

    def add_formatter(self, formatter=None):
        if formatter:
            self.formatters += listify(formatter)

    def add_filter(self, filter=None, rule=None):
        if filter:
            if rule:
                if isinstance(filter, list):
                    raise Exception("Filter should be callable if rule is specified. ")
                _filter = lambda *args, **kwargs: rule(filter(*args, **kwargs))
            else:
                _filter = filter
            self.filters += listify(_filter)

    def add_callback(self, callback=None):
        if callback:
            self.callbacks += listify(callback)

    def __call__(self, *args, **kwargs):
        for formatter in self.formatters:
            args, kwargs = formatter(*args, **kwargs)
        if all(_filter(*args, **kwargs) for _filter in self.filters):
            return self.reducer(
                [callback(*args, **kwargs) for callback in self.callbacks]
            )

    def call(self, *args, **kwargs):
        return self.__call__(*args, **kwargs)

    async def aiocall(self, *args, **kwargs):
        for formatter in self.formatters:
            args, kwargs = formatter(*args, **kwargs)
        if all(_filter(*args, **kwargs) for _filter in self.filters):
            res = []
            for callback in self.callbacks:
                if is_aio(callback):
                    if hasattr(callback, "aiocall"):
                        res.append(await callback.aiocall(*args, **kwargs))
                    else:
                        res.append(await callback(*args, **kwargs))
                else:
                    res.append(callback(*args, **kwargs))
            return self.reducer(res)

    def has_coroutine(self):
        return any(inspect.iscoroutinefunction(callback) for callback in self.callbacks)

    def __repr__(self):
        vals = []
        vals += [f"Formatter: {formatter}" for formatter in self.formatters]
        vals += [f"Filter: {filter}" for filter in self.filters]
        vals += [f"Callback: {callback}" for callback in self.callbacks]
        return "\n".join(vals)

    def __str__(self):
        return self.__repr__()
