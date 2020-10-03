import os
from utils_ak.data.df.file.stream import StreamFile
import pandas as pd
from utils_ak.os_tools import rename, remove


class ParquetFile(StreamFile):
    def __init__(self, fn, buffer_size=None, buffer_timeout=60, compression='snappy', engine='auto'):
        self.fn = fn
        self.swap1_fn = fn + '.swap1'
        self.swap2_fn = fn + '.swap2'
        self.engine = engine
        self.compression = compression
        super().__init__(buffer_size, buffer_timeout)

    def append(self, df, append=True):
        if append and os.path.exists(self.fn):
            old_df = self.read()
            df = pd.concat([old_df, df], axis=0)
        if not os.path.exists(self.fn):
            df.reset_index().to_parquet(self.fn, engine=self.engine, compression=self.compression)
        else:
            df.reset_index().to_parquet(self.swap1_fn, engine=self.engine, compression=self.compression)
            rename(self.fn, self.swap2_fn)
            rename(self.swap1_fn, self.fn)
            remove(self.swap2_fn)

    def append_stream(self, df):
        super().append_stream(df, 'default')

    def read(self):
        return pd.read_parquet(self.fn, engine=self.engine).set_index('index')
