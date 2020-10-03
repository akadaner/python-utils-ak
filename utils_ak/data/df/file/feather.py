import os
from utils_ak.data.df.file.stream import StreamFile
import pandas as pd


# todo: add test for extensability
# todo: debug


class FeatherFile(StreamFile):
    def __init__(self, fn, buffer_size=None, buffer_timeout=60, nthreads=1):
        self.fn = fn
        self.nthreads = nthreads
        super().__init__(buffer_size, buffer_timeout)

    def append(self, df, append=True):
        if append and os.path.exists(self.fn):
            old_df = self.read()
            df = pd.concat([old_df, df], axis=0)
        df.reset_index().to_feather(self.fn)

    def append_stream(self, df):
        super().append_stream(df, 'default')

    def read(self):
        return pd.read_feather(self.fn, self.nthreads).set_index('index')
