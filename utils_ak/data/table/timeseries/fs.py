class FsTimeSeriesTable:
    def __init__(self):
        pass

    def store(self, ts, key, msg):
        pass

    def store_many(self, values):
        for ts, key, msg in values:
            self.store(ts, key, msg)

    def clear(self):
        pass
