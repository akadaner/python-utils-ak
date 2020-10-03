from utils_ak.time.dt import *
from utils_ak.os_tools import list_files, remove, makedirs
from utils_ak.data.df.gran_df.granular import GranularFeather, GranularCSV, GranularMsgPack, GranularParquet
from utils_ak.data.df.gran_df.granular import LEVEL_ZERO, LEVEL_YEAR, LEVEL_HOUR
from utils_ak.data.df.gran_df.streamer import GranularStreamer
import numpy as np
import os

# todo: remove mode='w' for safety?


class GranularStorage(object):
    def __init__(self, root='.'):
        self.root = self._fix_root(root)
        makedirs(os.path.dirname(self.root))

    def __getitem__(self, item):
        return self.get_granular(item)

    def granular_names(self):
        fns = list_files(self.root, pattern='*.gran_df', recursive=True)
        names = [os.path.dirname(fn) for fn in fns]
        names = [self._fix_sep(name) for name in names]
        names = [name.replace(self.root, '') for name in names]
        return names

    def exists(self, path):
        return os.path.exists(self._metadata_fn(path))

    def remove(self, path):
        remove(self._full_path(path))

    def _full_path(self, path):
        path = self._fix_sep(path)
        if path.startswith(self.root):
            return path
        return os.path.join(self.root, path)

    def get_granular(self, path, level=None, pattern=None, mode='a'):
        return self.cls()(self._full_path(path), level, pattern, mode=mode)

    def cls(self):
        raise NotImplementedError('Not implemented')

    def _metadata_fn(self, path):
        return os.path.join(self._full_path(path), '.gran_df')

    def _fix_sep(self, path):
        path = path.replace('\\', '/')
        path = path.replace(os.path.sep, '/')
        return path

    def _fix_root(self, root):
        if root[-1] != '/':
            root += '/'
        return self._fix_sep(root)

    def get_streamer(self, path, level=3, stream_level=3, **kwargs):
        return GranularStreamer(self, path, level=level, stream_level=stream_level, **kwargs)


class GranularFeatherStorage(GranularStorage):
    def cls(self):
        return GranularFeather


class GranularCSVStorage(GranularStorage):
    def cls(self):
        return GranularCSV


class GranularMsgPackStorage(GranularStorage):
    def cls(self):
        return GranularMsgPack


class GranularParquetStorage(GranularStorage):
    def cls(self):
        return GranularParquet


if __name__ == '__main__':
    gs = GranularCSVStorage('gs_root')

    gran1 = gs.get_granular('gran1', level=LEVEL_ZERO)
    gran2 = gs.get_granular('gran2', level=LEVEL_YEAR)
    gran3 = gs.get_granular('gran3', level=LEVEL_HOUR)

    index = [cast_datetime('2018.05.01') + timedelta(days=1) * i for i in range(3)]
    df1 = pd.DataFrame(np.random.randn(3, 1), index=index, columns=['0'])

    gran1.append(df1, 'foo1')
    gran2.append(df1, 'foo2')
    gran3.append(df1, 'foo3')

    print(gs.granular_names())

    for name in gs.granular_names():
        gran = gs[name]
        for key in gran.keys():
            print(key)
            print(gran[key].head(3))

    gs.remove('gran3')
    print(gs.granular_names())
