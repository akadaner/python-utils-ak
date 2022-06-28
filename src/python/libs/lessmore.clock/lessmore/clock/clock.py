import json
import time

import pandas as pd
from lessmore.code_block import code


class Clock:
    """ Time measurement of any code piece. """

    def __init__(
        self,
        checkpoints=None,
        laps=None,
        auto_close=False,
        sep="/",
        max_observations=100000,
        enabled=True,
    ):
        self.checkpoints = checkpoints or {}
        self.laps = laps or {}

        self.auto_close = auto_close
        self.last_key = None

        self.prefix = None
        self.sep = sep

        self.max_observations = max_observations

        self.enabled = enabled

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False

    def set_prefix(self, prefix=None):
        if not prefix:
            self.prefix = None
        else:
            prefix = prefix if isinstance(prefix, list) else [prefix]
            self.prefix = self.sep.join(prefix)

    def add_lap(self, key, lap):
        self.laps.setdefault(key, []).append(lap)
        if len(self.laps[key]) > self.max_observations:
            self.laps[key] = self.laps[key][-self.max_observations :]

    def clock(self, key="clock", reset=False):
        if not self.enabled:
            return

        if isinstance(key, list):
            key = self.sep.join(key)

        if self.prefix:
            key = self.sep.join([self.prefix, key])

        if reset:
            self.pop(key)
        t = time.perf_counter()
        key = str(key)

        if key not in self.checkpoints:
            self.checkpoints[key] = t

            if self.auto_close and self.last_key:
                self.add_lap(self.last_key, t - self.checkpoints.pop(self.last_key))
        else:
            self.add_lap(key, t - self.checkpoints.pop(key))
        self.last_key = key
        return ContextClockWithKey(key, self)

    def stop(self, key=None, raise_if_not_started=True):
        """
        :param key: str.
        :param raise_if_not_started:
        :return: bool. Raise if exists
        """
        if not self.enabled:
            return

        t = time.perf_counter()
        if not key:
            for key, t0 in self.checkpoints.items():
                self.add_lap(key, t - t0)
            self.checkpoints = {}
        else:
            if key in self.checkpoints:
                self.add_lap(key, t - self.checkpoints.pop(key))
            elif raise_if_not_started:
                raise Exception("Key {} is not started".format(key))

    def start(self, key, raise_if_already_started=True):
        """
        :param key: str.
        :param raise_if_already_started:  bool. Raise if exists
        :return:
        """
        if not self.enabled:
            return

        if key not in self.checkpoints:
            return self.clock(key)
        elif raise_if_already_started:
            raise Exception("Key {} has already started".format(key))

    def stats(self):
        keys = list(self.laps.keys())
        if not keys:
            return pd.DataFrame(index=["count", "mean", "sum", "max", "min"])
        df = pd.concat([pd.DataFrame({k: self.laps[k]}) for k in keys], ignore_index=True, axis=1)
        df.columns = keys
        stats_df = df.agg(["count", "mean", "sum", "max", "min"])
        return stats_df

    def pop(self, key):
        self.checkpoints.pop(key, None)
        self.laps.pop(key, None)

    def reset(self):
        self.checkpoints = {}
        self.laps = {}

    def print_trie(self, by="sum", sep=None, precision=3):
        sep = sep or self.sep

        import pygtrie

        t = pygtrie.StringTrie(separator=sep)

        stats = self.stats().T[[by]]
        for key, row in stats.sort_index().iterrows():
            t[key] = row[by]

        def is_child(key):
            return not t.has_subtrie(key)

        def depth_print(s, depth=0):
            print("\t" * depth + s)

        def print_trie(path_conv, path, children, value=None):
            path = path_conv(path)
            depth = path.count(sep)

            if t.has_key(path):
                value_repr = value if not precision else round(value, precision)
                path_repr = path.split("/")[-1]
                if is_child(path):
                    # child
                    depth_print("{}: {}".format(path_repr, value_repr), depth)
                else:
                    depth_print("{}: {}".format(path_repr, value_repr), depth)
            for child in children:
                pass
            return value

        def add_other(path_conv, path, children, value=None):
            path = path_conv(path)
            if value and not is_child(path):
                other_key = "/".join([path, "other"])
                if not t.has_key(other_key):
                    t[other_key] = value - sum(child for child in children)
            else:
                for child in children:
                    pass
            return value

        if by == "sum":
            t.traverse(add_other)
        t.traverse(print_trie)

    def __call__(self, *args, **kwargs):
        return self.clock(*args, **kwargs)

    def __getitem__(self, item):
        return self.stats().T["sum"][item]

    @property
    def state(self):
        return {"laps": self.laps}

    def save(self, fn):
        with open(fn, "w") as f:
            json.dump(self.state, f, indent=1)

    def load(self, fn):
        with open(fn, "r") as f:
            js = json.load(f)
            self.laps = js["laps"]


clock = Clock(enabled=False)


class ContextClockWithKey:
    def __init__(self, key, clock=clock):
        self.clock = clock
        self.key = key

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clock.stop(self.key)


from functools import wraps


def clockify(key=None):
    def _clockify(f):
        @wraps(f)
        def inner(*args, **kwargs):

            _values = [str(f.__module__), f.__name__]
            if key:
                _values.append(key)
            _key = "/".join(_values)

            clock.start(_key)

            with code("tmp code"):  # todo next: del
                try:
                    res = f(*args, **kwargs)
                    clock.stop(_key)
                except:
                    clock.stop(_key)
                    raise
            return res

        return inner

    return _clockify
