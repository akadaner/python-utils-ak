

import pandas as pd


class PandasWriter:
    def __init__(self, fn, flush_buffer=100000):
        self.flush_buffer = flush_buffer
        self.fn = fn
        self.buffer_values = []

    def write_header(self, header):
        with open(self.fn, 'w') as f:
            f.write(','.join(header) + '\n')

    def write_values(self, values):
        self.buffer_values.extend(values)

        if self.flush_buffer is not None and len(values) >= self.flush_buffer:
            self.flush()

    def flush(self):
        pd.DataFrame(self.buffer_values).to_csv(self.fn, mode='a', index=False, header=False)
        self.reset_buffer()

    def reset_buffer(self):
        self.buffer_values = []
