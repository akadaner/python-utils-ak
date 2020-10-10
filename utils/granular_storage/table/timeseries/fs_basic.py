from utils.granular_storage.table.timeseries.fs import FsTimeSeriesTable
from utils.os_tools import makedirs, open_atomic, remove


class BasicFsTimeSeriesTable(FsTimeSeriesTable):
    def __init__(self, fn):
        super().__init__()
        self.fn = fn
        makedirs(self.fn)

        self._f = None

    def encode(self, msg):
        raise NotImplementedError

    def read_file(self, f):
        raise NotImplementedError

    @property
    def f(self):
        if not self._f:
            self._f = open(self.fn, 'ab')
        return self._f

    def store(self, ts, key, msg):
        self.f.write(self.encode(msg))

    def flush(self):
        self.f.flush()

    def close(self):
        if self._f:
            self._f.close()
            self._f = None

    def clear(self):
        self.close()
        remove(self.fn)

    def store_many(self, values, safe=False, overwrite=False):
        """
        :param values: [[ts, key, msg], ...]
        :param safe: will make copy first, write to copy and then rename to initial file. Useful when we don't want granular_storage to be crashed at any cost.
        :return:
        """
        # remove ts and key from values (not used)
        values = [value[2] for value in values]

        if overwrite:
            self.clear()

        if not safe:
            for msg in values:
                self.store(None, None, msg)
        else:
            # append old table first
            self.close()
            values = list(self.read()) + values

            with open_atomic(self.fn, 'wb', fsync=True) as f:
                for msg in values:
                    f.write(self.encode(msg))

    def read(self):
        raise NotImplementedError
