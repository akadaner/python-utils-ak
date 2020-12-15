import pandas as pd

from utils_ak.split_file import SplitFile


class PandasCSVWriter:
    def __init__(self, fn, buffer_length=1e5, max_file_size=1e9):
        self.buffer_length = buffer_length
        self.fn = fn
        self.buffer_values = []
        self.max_file_size = max_file_size

    def write_header(self, header):
        with open(self.fn, 'w') as f:
            f.write(','.join(header) + '\n')

    def write_values(self, values):
        self.buffer_values.extend(values)

        if self.buffer_length is not None and len(values) >= self.buffer_length:
            self.flush()

    def flush(self):
        sf = SplitFile(self.fn)
        fn = sf.get_current(max_size=self.max_file_size)
        pd.DataFrame(self.buffer_values).to_csv(fn, mode='a', index=False, header=False)
        self.reset_buffer()

    def reset_buffer(self):
        self.buffer_values = []
