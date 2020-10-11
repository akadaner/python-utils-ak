import os

from utils_ak.os.os import makedirs
from utils_ak.time.dt import cast_datetime
from utils_ak.dict.limited_dict import LimitedDict

import time


class Storage:
    def __init__(self, root):
        self.root = root
        makedirs(root)

    def __getitem__(self, db_name):
        return self.get_db(db_name)

    @property
    def table_cls(self):
        raise NotImplemented

    @property
    def db_cls(self):
        raise NotImplemented

    @property
    def ext(self):
        return '.txt'

    def get_db(self, db_name, *args, **kwargs):
        return self.db_cls(os.path.join(self.root, db_name), self.table_cls, self.ext, *args, **kwargs)


class DB:
    def __init__(self, path, table_cls, ext):
        self.path = path
        self.table_cls = table_cls
        self.ext = ext

    def get_table(self, table_name, *args, **kwargs):
        return self.table_cls(os.path.join(self.path, table_name + self.ext), *args, **kwargs)

    def __getitem__(self, table_name):
        return self.get_table(table_name)


class GranularTable(object):
    """ This class is a decorator that makes any table granulared """
    def __init__(self, fn, pattern, cache_size=100, *args, **kwargs):
        self.table_args = args
        self.table_kwargs = kwargs

        self.dirname = os.path.dirname(fn)
        self.basename = os.path.basename(fn)
        self.base, self.ext = os.path.splitext(self.basename)
        self.pattern = pattern
        self.tables = {}

        self._memo = LimitedDict(cache_size)

    def store(self, ts, key, message):
        if ts is None:
            ts = time.time()
        self._get_atom_table(ts, key).store(ts, key, message)

    def store_many(self, values, *args, **kwargs):
        values_by_table = {}
        for ts, key, msg in values:
            table = self._get_atom_table(ts, key)
            values_by_table.setdefault(table, []).append([ts, key, msg])

        for table, table_values in values_by_table.items():
            table.store_many(table_values, *args, **kwargs)

    def _get_granular_fn(self, ts, key):
        _cache_name = str(ts) + "|" + str(key)
        if _cache_name in self._memo:
            return self._memo[_cache_name]

        fn = cast_datetime(ts).strftime(self.pattern)
        fn = fn.format(key=key, base=self.base)
        result = os.path.join(self.dirname, fn + self.ext)
        self._memo[_cache_name] = result
        return result

    def _get_atom_table(self, ts, key):
        fn = self._get_granular_fn(ts, key)
        if fn not in self.tables:
            self.tables[fn] = self.table_cls(fn, *self.table_args, **self.table_kwargs)
        return self.tables[fn]

    def close(self):
        for name, table in self.tables.items():
            table.close()

    def flush(self):
        for name, table in self.tables.items():
            table.flush()

    @property
    def table_cls(self):
        raise NotImplemented()


