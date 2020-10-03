import msgpack
import os
from utils_ak.data.table.timeseries.fs_basic import BasicFsTimeSeriesTable


class FsMsgPackTimeSeriesTable(BasicFsTimeSeriesTable):
    def __init__(self, fn, encoding='utf-8'):
        super().__init__(fn)
        self.fn = fn
        self.encoding = encoding

    def encode(self, msg):
        return msgpack.packb(msg, use_bin_type=True)

    def read(self):
        if not os.path.exists(self.fn):
            raise StopIteration

        with open(self.fn, 'rb') as f:
            for unpacked in msgpack.Unpacker(f, raw=False):
                yield unpacked


if __name__ == '__main__':
    from datetime import datetime

    storage = FsMsgPackTimeSeriesTable('tmp2.tss')

    for i in range(3):
        storage.store(datetime.now().timestamp(), 'key', {'a': i})

    # flush
    storage.close()
    for value in storage.read():
        print(1, value)
    print()

    storage.store_many([[datetime.now().timestamp(), 'key', {'b': i}] for i in range(3)])

    # flush
    storage.close()

    for value in storage.read():
        print(2, value)
    print()

    storage.store_many([[datetime.now().timestamp(), 'key', {'c': i}] for i in range(3)], safe=True, overwrite=True)

    # already flushed!

    for value in storage.read():
        print(3, value)
    print()
    storage.clear()
