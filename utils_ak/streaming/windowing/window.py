from utils_ak.builtin import update_dic


class Window:
    def __init__(self):
        self.state = 'open' # 'open', 'closed'

    def close(self):
        self.state = 'closed'

    def is_closeable(self):
        # check if we can close the window now
        raise NotImplemented()


class IntervalWindow(Window):
    def __init__(self, beg, end):
        super().__init__()
        self.beg = beg
        self.end = end


class CollectorWindow(Window):
    def __init__(self, window_id, fields, sources=None):
        super().__init__()
        self.window_id = window_id
        self.filled = {}  # {field: {symbol: value}}
        self.fields = fields
        self.sources = sources or []

    def add(self, values, source=None):
        if source and source not in self.sources:
            return

        if self.state == 'closed':
            raise Exception('Cannot add to a closed window')

        values = {k: v for k, v in values.items() if k in self.fields}  # remove extra fields

        self.filled = update_dic(self.filled, values)

    def is_closeable(self):
        return set(self.filled.keys()) == set(self.fields)

    def close(self):
        self.state = 'closed'
        return self.filled



if __name__ == '__main__':
    collector = CollectorWindow('window_id', fields=['a', 'b'])

    print(collector.add({'a': 1}))
    print(collector.is_closeable(), collector.state)
    print(collector.add({'b': 1}))
    print(collector.is_closeable(), collector.state)
    print(collector.close())
    print(collector.is_closeable(), collector.state)

