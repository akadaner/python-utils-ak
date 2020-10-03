import pandas as pd
from datetime import datetime
from utils_ak.time.dt import cast_timedelta


# todo: specify columns and data_columns for efficiency
# todo: inherit from streamfile somehow


class HDFFile(pd.HDFStore):
    """ A simple wrapper for pd.HDFStore for dumping streaming data. """

    def __init__(self, path, mode='a', complevel=9, complib='blosc:lz4hc', buffer_size=None, buffer_timeout=60, extensible=True, **kwargs):
        self.buffer_size = buffer_size or 0
        self.cur_size = 0
        self.buffer_timeout = cast_timedelta(buffer_timeout)

        # {key: [value1, value2, ...]}
        self.buffer = {}
        self.last_flush = datetime.now()
        self.extensible = extensible
        super().__init__(path, mode, complevel, complib, **kwargs)

    def close(self):
        self.flush_buffer()
        super().close()

    def append_stream(self, df, table):
        if len(df) == 0:
            raise Exception('Cannot add an empty dataframe')

        self.buffer.setdefault(table, []).append(df)
        self.cur_size += len(df)

        if self.is_buffer_full():
            self.flush_buffer()

    def is_buffer_full(self):
        if self.buffer_timeout:
            if datetime.now() - self.last_flush > self.buffer_timeout:
                return True
        return self.cur_size > self.buffer_size

    def append(self, df, table, append=True, **kwargs):
        try:
            super().append(table, df, format='table', append=append, **kwargs)
        except ValueError:
            if not self.extensible:
                raise
            df = pd.concat([self[table], df], axis=0)
            super().append(table, df, format='table', append=False, **kwargs)

    def flush_buffer(self):
        for table, dfs in self.buffer.items():
            df = pd.concat(dfs, axis=0)
            self.append(df, table)

        self.buffer = {}
        self.last_flush = datetime.now()
        self.cur_size = 0

    def getdefault(self, key, default=None):
        if key in self:
            return self[key]
        return default


def ex():
    import numpy as np
    from datetime import datetime, timedelta
    import os

    if not os.path.exists('data/'):
        os.makedirs('data/')

    df1 = pd.DataFrame(np.random.randn(2, 1), index=[datetime.now() + timedelta(days=1) * i for i in range(2)])
    print('Dataframe 1')
    print(df1)

    with HDFFile('data/test.h5', mode='w', buffer_size=3) as hdf:
        hdf.append_stream(df1, 'backtest/trades')
        hdf.append_stream(df1, 'backtest/trades')
        # flushing is here
        hdf.append_stream(df1, 'backtest/trades')
        print('Current keys: {}'.format(hdf.keys()))

    # flushing is on the close of file

    with HDFFile('data/test.h5') as hdf:
        # 6 values here
        print(hdf['backtest/trades'])
        print(hdf.keys())

    # extension test
    df2 = pd.DataFrame(np.random.randn(2, 2), index=[datetime.now() + timedelta(days=1) * i for i in range(2)])

    with HDFFile('data/test.h5', mode='a') as hdf:
        hdf.append_stream(df2, 'backtest/trades')
        print(hdf['backtest/trades'])

    with HDFFile('data/test.h5', mode='a') as hdf:
        hdf.append_stream(df2, 'backtest/trades')
        print(hdf['backtest/trades'])


if __name__ == '__main__':
    ex()
