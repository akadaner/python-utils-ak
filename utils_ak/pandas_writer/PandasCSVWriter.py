import pandas as pd
import os

from utils_ak.split_file import SplitFile


class PandasCSVWriter:
    def __init__(self, fn, buffer_length=1e5, max_file_size=1e9):
        self.buffer_length = buffer_length
        self.fn = fn
        self.buffer_values = []
        self.max_file_size = max_file_size
        self.header = None

    def write_header(self, header):
        self.header = header

    def write_values(self, values):
        self.buffer_values.extend(values)

        if self.buffer_length is not None and len(values) >= self.buffer_length:
            self.flush()

    def flush(self):
        sf = SplitFile(self.fn)
        fn = sf.get_current(max_size=self.max_file_size)
        if os.path.exists(fn):
            pd.DataFrame(self.buffer_values, columns=self.header).to_csv(fn, mode='a', index=False, header=False)
        else:
            pd.DataFrame(self.buffer_values, columns=self.header).to_csv(fn, mode='a', index=False)
        self.reset_buffer()

    def reset_buffer(self):
        self.buffer_values = []
