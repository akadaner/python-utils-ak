import os
import pandas as pd
from datetime import timedelta

from utils.granular_storage.df.file.stream import StreamFile
from utils.time.dt import cast_datetime
from utils.os_tools import rename, remove

import numpy as np


# todo: safe renaming and removing?


class MsgPackFile(StreamFile):
    def __init__(self, fn, buffer_size=None, buffer_timeout=60):
        self.fn = fn
        self.swap1_fn = fn + '.swap1'
        self.swap2_fn = fn + '.swap2'
        super().__init__(buffer_size, buffer_timeout)

    def append(self, df, append=True):
        if append and os.path.exists(self.fn):
            old_df = self.read()
            df = pd.concat([old_df, df], axis=0, sort=False)
        if not os.path.exists(self.fn):
            df.reset_index().to_msgpack(self.fn)
        else:
            df.reset_index().to_msgpack(self.swap1_fn)
            rename(self.fn, self.swap2_fn)
            rename(self.swap1_fn, self.fn)
            remove(self.swap2_fn)

    def append_stream(self, df):
        super().append_stream(df, 'default')

    def read(self):
        return pd.read_msgpack(self.fn).set_index('index')

    def ext(self):
        return '.msg'


if __name__ == '__main__':
    index = [cast_datetime('2018.05.01') + timedelta(hours=6) * i for i in range(3)]
    df = pd.DataFrame(np.random.randn(3, 1), index=index, columns=['0'])
    file = MsgPackFile('granular_storage/test.msg')

    print(file.read())
    file.append(df)
    print(file.read())
