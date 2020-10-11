from utils_ak.granular_storage.df.file.stream import StreamFile
from utils_ak.time.dt import *

import pandas as pd
import os


# todo: debug
# todo: add extensibility

class CSVFile(StreamFile):
    def __init__(self, fn, buffer_size=None, buffer_timeout=60, compression=None, extensible=True):
        self.fn = fn
        self.compression = compression
        self.extensible = extensible
        super().__init__(buffer_size, buffer_timeout)

    def _preprocess(self, df):
        # todo: convert datetime stuff
        df = df.reset_index()
        df['index'] = df['index'].apply(lambda dt: int(cast_mts(dt)))
        df.columns = [str(col) for col in df.columns]
        return df

    def append(self, df, append=True):
        if self.extensible:
            if append and os.path.exists(self.fn):
                df.columns = [str(col) for col in df.columns]
                old_df = self.read()
                df = pd.concat([old_df, df], axis=0)
            df = self._preprocess(df)
            df.to_csv(self.fn, mode='w', compression=self.compression, index=False, header=True)
        else:
            mode = 'a' if append else 'w'
            write_header = mode == 'w' or not os.path.exists(self.fn)
            df = self._preprocess(df)
            df.to_csv(self.fn, mode=mode, compression=self.compression, index=False, header=write_header)

    def append_stream(self, df):
        super().append_stream(df, 'default')

    def read(self, index_col='index'):
        df = pd.read_csv(self.fn)
        df[index_col] = cast_datetime_series(df[index_col])
        df.set_index([index_col], inplace=True)
        df.index.name = 'index'
        df.columns = [str(col) for col in df.columns]
        return df

    def ext(self):
        return '.csv.gz'
