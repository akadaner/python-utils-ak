import os
import re
import pandas as pd
import json

from datetime import datetime, timedelta

from utils_ak.data.df.file import CSVFile, FeatherFile, MsgPackFile, ParquetFile
from utils_ak.data.df.gran_df.enums import *

from utils_ak.time.dt import cast_timedelta, cast_datetime
from utils_ak.os_tools import list_files, makedirs, remove
from utils_ak.pd_tools import merge
from utils_ak import re_tools


# todo: optimize upd_struct usage. Minimize its usage
# todo: parse level and pattern without metadata presetn


class NoFilesException(Exception):
    pass


class Granular(object):
    default_patterns = {LEVEL_ZERO: '{key}',
                        LEVEL_YEAR: '%Y/%Y_{key}',
                        LEVEL_MONTH: '%Y/%m/%Y%m_{key}',
                        LEVEL_DAY: '%Y/%m/%d/%Y%m%d_{key}',
                        LEVEL_HOUR: '%Y/%m/%d/%H/%Y%m%d%H_{key}'}

    def __init__(self, root, level=None, pattern=None, mode='a', open_files_timeout=600, buffer_size=10000, buffer_timeout=60):
        self.root = self._fix_root(root)

        self.mode = mode
        if mode == 'w':
            remove(self.root)
        makedirs(self.root)

        # put level and pattern into metadata
        # there is 2 formats for granular name due to refactoring bug
        fn1 = os.path.join(self.root, '.gran')
        fn2 = os.path.join(self.root, '.gran_df')
        self.metadata_fn = fn2 if os.path.exists(fn2) else fn1

        self.metadata = {}
        if os.path.exists(self.metadata_fn):
            self.load_metadata()

            if level is not None and self.level != level:
                raise Exception(f'Level is already initialized and is different {self.root}')

            if pattern and self.pattern != pattern:
                raise Exception(f'Pattern is already initialized and is different {self.root}')
        else:
            if level is None:
                raise Exception('Granular not initialized. Specify level')
            self.level = level
            self.pattern = pattern or self.default_patterns[level]
            self.pattern += self.ext
            self.dump_metadata()

        self.level_keys = LEVEL_KEYS[:self.level]

        self.open_files_timeout = cast_timedelta(open_files_timeout)

        if not self._is_pattern_valid(self.pattern):
            raise Exception(f'Pattern {self.pattern} is not valid for level {self.level}')

        self._struct_df = None

        # stream
        # {fn: `HDFFile`}
        self.files = {}
        # {fn: `datetime`}
        self.last_used = {}
        self.buffer_size = buffer_size
        self.buffer_timeout = buffer_timeout

    @property
    def ext(self):
        raise NotImplementedError('Extension not specified')

    def load_metadata(self):
        with open(self.metadata_fn, 'r') as f:
            self.metadata = json.load(f)
        self.level = self.metadata['level']
        self.pattern = self.metadata['pattern']

    def dump_metadata(self):
        self.metadata.update({'level': self.level,
                              'pattern': self.pattern,
                              'ext': self.ext,
                              'storage_type': 'granular'})
        with open(self.metadata_fn, 'w') as f:
            json.dump(self.metadata, f, indent=1)

    def upd_metadata(self, md):
        self.metadata.update(md)
        self.dump_metadata()

    def init_file(self, fn, buffer_size=10000, buffer_timeout=60):
        raise NotImplementedError('Not implemented')

    def iter_granularity(self, df):
        # todo: optimize: do not create a copy!
        df = df.copy()

        granular_keys = [f'_{i}' for i in range(self.level)]
        full_keys = ['year', 'month', 'day', 'hour']
        for i in range(self.level):
            df[granular_keys[i]] = getattr(df.index, full_keys[i])

        if self.level == LEVEL_ZERO:
            fn = os.path.join(self.root, self.pattern)
            yield fn, df
        else:
            for ind, grp in df.groupby(granular_keys):
                ind = ind if isinstance(ind, (list, tuple)) else [ind]
                # fit date
                fn = self.pattern
                for i in range(self.level):
                    fn = fn.replace(self.level_keys[i], str(ind[i]).zfill(2))
                fn = os.path.join(self.root, fn)
                # todo: optimize drop - no copying!
                yield fn, grp.drop(granular_keys, axis=1)

    def remove(self, key):
        struct_df = self.get_struct_df(key)
        fns = struct_df['fn'].unique()
        if fns:
            for fn in fns:
                os.remove(fn)
            self.reset_struct_df()

    def append(self, df, key, append=True, append_single=True):
        """
        :param df: `pd.DataFrame`
        :param key: str
        :param table: str
        :param append: bool. When False, the function will delete table, before appending new data
        :param append_single: bool. Similar to append, but is relevant only to the files, that are relevant for current df (withing it's timerange)
        :return:
        """
        if not append:
            self.remove(key)

        for fn, grp in self.iter_granularity(df):
            fn = fn.format(key=key)
            self._makedirs(fn)
            self._append_single(grp, fn, append_single)

        # todo: optimize
        self.reset_struct_df()

    def _append_single(self, df, fn, append=True):
        with self.init_file(fn) as f:
            f.append(df, append=append)

    # stream
    def append_stream(self, df, key):
        for fn, grp in self.iter_granularity(df):
            fn = fn.format(key=key)
            self._makedirs(fn)
            if fn not in self.files:
                self.files[fn] = self.init_file(fn, buffer_size=self.buffer_size, buffer_timeout=self.buffer_timeout)
            self.files[fn].append_stream(grp)
            self.last_used[fn] = datetime.now()
        self.close_stalled_files()

        # todo: optimize
        self.reset_struct_df()

    def close_stalled_files(self):
        for fn, f in dict(self.files).items():
            if fn in self.last_used and datetime.now() - self.last_used[fn] > self.open_files_timeout:
                f.close()
                self.files.pop(fn)
                self.last_used.pop(fn, None)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        for f in self.files.values():
            f.close()
        self.files = {}
        self.last_used = {}

    def flush(self):
        for f in self.files.values():
            f.flush_buffer()
        self.reset_struct_df()

    def __getitem__(self, key):
        return self.read(key)

    def read(self, key, beg=None, end=None):
        gran_df = self.get_struct_df(key)
        if self.level == LEVEL_ZERO:
            gran_df = gran_df[gran_df['ts'] == '']

            if gran_df is None or len(gran_df) == 0:
                raise NoFilesException('No files found at location for specified period')

            if len(gran_df) != 1:
                raise Exception('Bad file structure')

            fn = gran_df.iloc[0]['fn']
            with self.init_file(fn) as f:
                return f.read()

        if gran_df is None or len(gran_df) == 0:
            raise NoFilesException('No files found at location for specified period')

        # todo: make properly
        ts_len = len(gran_df['ts'].iloc[0])

        # filter tables by beg and end
        if beg:
            try:
                beg = cast_datetime(beg).strftime('%Y%m%d%H%M%S')
            except:
                if not isinstance(beg, str):
                    # beg may be like 201801, not a datetime object
                    raise
            beg = beg[:ts_len]
            gran_df = gran_df[gran_df['ts'] >= beg]

        if end:
            try:
                end = cast_datetime(end).strftime('%Y%m%d%H%M%S')
            except:
                if not isinstance(end, str):
                    # end may be like 201801, not a datetime object
                    raise
            end = end[:ts_len]
            gran_df = gran_df[gran_df['ts'] <= end]

        if len(gran_df) == 0:
            raise NoFilesException('No files found at location for specified period')

        # collect and concat all dataframes
        dfs = []
        for fn, grp in gran_df.groupby(by='fn'):
            with self.init_file(fn) as f:
                dfs.append(f.read())
        df = pd.concat(dfs, axis=0)
        return df

    def merge(self, df, key, by=None, by_index=False, keep='last'):
        """
        :param df:
        :param key:
        :param by:
        :param keep: str. 'last' or 'first' (keep old or new duplicated values)
        :return:
        """
        beg, end = df.index[0], df.index[-1]
        try:
            # beg and end are inclusive here
            cur_df = self.read(key, beg, end)
        except NoFilesException:
            cur_df = pd.DataFrame()

        if len(cur_df) > 0:
            df = merge([cur_df, df], by=by, by_index=by_index, keep=keep)

        self.append(df, key, append_single=False)

    def reset_struct_df(self):
        self._struct_df = None

    def get_struct_df(self, key=None):
        if self._struct_df is None:
            self.upd_struct()
        df = self._struct_df
        if key:
            df = df[df['key'] == key]
        return df.sort_values(by='ts')

    def keys(self):
        return list(self.get_struct_df()['key'].unique())

    def upd_struct(self):
        """ Update self._struct_df for reading. """
        values = []
        for fn in list_files(self.root, recursive=True):
            if self._is_system_file(fn.replace(self.root, '')):
                continue

            fn = self._fix_sep(fn)

            res = self._parse_granularity(fn)
            if not res:
                continue
            values.append(res)

        df = pd.DataFrame(values, columns=['fn', 'ts', 'key'])
        self._struct_df = df
        return self._struct_df

    @staticmethod
    def _is_system_file(fn):
        while fn:
            if os.path.basename(fn) in ['.gran_df', '.stream']:
                return True
            fn = os.path.dirname(fn)
        return False

    def _parse_granularity(self, path):
        """
        :param path: '20180315_filename.h5/some/table' -> [<full_path>, '20180315', filename.h5, /some/table] if self.pattern = '%Y%m%d_{fn}/{table}'
        :return:
        """
        y_pat = '\d{4}'
        m_pat = '\d{2}'
        d_pat = '\d{2}'
        h_pat = '\d{2}'
        key_pat = '[^/]+'
        ext_pat = f'\.{self.ext[1:]}$'

        pat = self.pattern
        pat = pat[:-len(self.ext)]
        pat = re_tools.replace_with_pattern(pat, '%Y', 'Y', y_pat)
        pat = re_tools.replace_with_pattern(pat, '%m', 'm', m_pat)
        pat = re_tools.replace_with_pattern(pat, '%d', 'd', d_pat)
        pat = re_tools.replace_with_pattern(pat, '%H', 'H', h_pat)
        pat = pat.replace('{key}', re_tools.def_group('key', key_pat))
        pat = pat + ext_pat

        base_path = path.replace(self.root, '')
        search = re.search(pat, base_path)
        if not search:
            return
        group = search.groupdict()

        ts = ''.join([group[level_key.replace('%', '')] for level_key in self.level_keys])
        return [path, ts, group['key']]

    def _fix_sep(self, path):
        path = path.replace('\\', '/')
        path = path.replace(os.path.sep, '/')
        return path

    def _fix_root(self, root):
        if root[-1] != '/':
            root += '/'
        return self._fix_sep(root)

    def _makedirs(self, path):
        cur_dir = os.path.dirname(path)
        if not os.path.exists(cur_dir):
            os.makedirs(cur_dir)

    def _is_pattern_valid(self, pattern):
        # todo: more extensive check
        if not all([level_key in pattern for level_key in self.level_keys]):
            return False
        if '{key}' not in pattern:
            return False
        return True


class GranularCSV(Granular):
    def init_file(self, fn, buffer_size=10000, buffer_timeout=60):
        return CSVFile(fn, buffer_size, buffer_timeout, compression=None)

    @property
    def ext(self):
        return '.csv'


class GranularFeather(Granular):
    def init_file(self, fn, buffer_size=1000, buffer_timeout=60):
        return FeatherFile(fn, buffer_size, buffer_timeout)

    @property
    def ext(self):
        return '.feather'


class GranularMsgPack(Granular):
    def init_file(self, fn, buffer_size=1000, buffer_timeout=60):
        return MsgPackFile(fn, buffer_size, buffer_timeout)

    @property
    def ext(self):
        return '.msg'


class GranularParquet(Granular):
    def init_file(self, fn, buffer_size=1000, buffer_timeout=60):
        return ParquetFile(fn, buffer_size, buffer_timeout)

    @property
    def ext(self):
        return '.parquet'


def ex_basic(granular=GranularCSV, key1='data', key2='data2'):
    import numpy as np
    print('Basic usage test')

    # note: mode='w' will delete all existing .h5 files in root directory! Be careful.
    gran = granular('data/root/', LEVEL_DAY, mode='w')
    index = [cast_datetime('2018.05.01') + timedelta(days=1) * i for i in range(3)]
    df1 = pd.DataFrame(np.random.randn(3, 1), index=index, columns=['0'])
    df2 = pd.DataFrame(np.random.randn(3, 2), index=index, columns=['0', '1'])
    print('Initial dataframes')
    print(df1)
    print(df2)

    print('Read')
    gran.append(df1, key1)

    print(gran[key1])
    gran.append(df2, key1)

    print(gran.read(key1))
    # or with __getitem__. They are the same
    print(gran[key1])

    print('Read with beg and end')
    print(gran.read(key1, beg='2018.05.01', end='2018.05.02'))

    print('Get keys')
    print(gran.keys())

    print('Write and read with extended data')
    gran.append(df2, key1)

    print(gran[key1])

    print('Level zero test')
    gran = granular('data/root/', LEVEL_ZERO, mode='w')
    gran.append(df1, key1)
    print(gran.get_struct_df(key1))
    print(gran[key1])
    gran.append(df2, key1)
    print(gran[key1])

    print('Stream examples')
    with granular('data/root/', LEVEL_DAY, mode='w') as gran:
        gran.append_stream(df1, key1)
        gran.append_stream(df1, key1)
        gran.append_stream(df2, key1)
        gran.append_stream(df2, key1)

    # with statement handles flush properly
    gran = granular('data/root/', LEVEL_DAY)
    print(gran[key1])

    print('Custom pattern')
    gran = granular('data/root', LEVEL_DAY, pattern='%Y/%Y%m%d_{key}', mode='w')
    gran.append(df1, key1)
    print(gran[key1])
    print(gran.get_struct_df())
    print(gran.get_struct_df().groupby('key').agg('last'))

    # append - false
    gran = granular('data/root', LEVEL_DAY, mode='w')
    tmp = df1.iloc[1:].copy()

    # test append=False
    print("Remove")
    gran.append(df1, key1)
    gran.append(tmp, key1, append=False)
    print(gran[key1])
    print(gran.get_struct_df())

    gran.remove(key1)
    print(gran.get_struct_df())

    print("Remove 2")
    gran.append(df1, key1)
    gran.append(df1, key2)
    gran.append(tmp, key1, append=False)
    print(gran[key1])
    print(gran[key2])
    print(gran.get_struct_df())

    print('Merge')
    gran = granular('data/root', LEVEL_DAY, mode='w')
    gran.append(df1, key1)
    tmp = df1.copy()
    tmp[:] = 0
    tmp = tmp.iloc[1:]
    gran.merge(tmp, key1, by_index=True, keep='last')
    print(gran[key1])


if __name__ == '__main__':
    print('GranularCSV' + ' - ' * 100)
    print()
    ex_basic(GranularCSV, 'data', 'data2')
    print()
    print('GranularFeather' + ' - ' * 100)
    print()
    ex_basic(GranularFeather, 'data', 'data2')
    print()
    print('GranularMsgPack' + ' - ' * 100)
    print()
    ex_basic(GranularMsgPack, 'data', 'data2')
