
from datetime import datetime
from utils_ak.time.dt import cast_timedelta
import pandas as pd

class StreamFile(object):
    """ A simple file with buffer support of dataframes """
    def __init__(self, buffer_size=None, buffer_timeout=60):
        self.buffer_size = buffer_size or 0
        self.cur_size = 0
        self.buffer_timeout = cast_timedelta(buffer_timeout)

        # {key: [value1, value2, ...]}
        self.buffer = {}
        self.last_flush = datetime.now()

    def append(self, *args, **kwargs):
        raise NotImplementedError('Not implemented')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.flush_buffer()

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

    def flush_buffer(self):
        for table, dfs in self.buffer.items():
            df = pd.concat(dfs, axis=0)
            self.append(df, table)

        self.buffer = {}
        self.last_flush = datetime.now()
        self.cur_size = 0
