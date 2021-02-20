from datetime import datetime
from utils_ak.builtin import update_dic


class Window:
    def __init__(self):
        self.state = 'open'  # 'open', 'closed'

    def close(self):
        self.state = 'closed'

    def is_closeable(self):
        # check if we can close the window now
        raise NotImplemented()


class ProcessingSessionWindow(Window):
    def __init__(self, gap):
        """
        :param gap: int, gap between sessions in seconds
        """
        super().__init__()
        self.values = []
        self.last_processing_time = None
        self.gap = gap

    def add(self, value):
        self.values.append(value)
        self.last_processing_time = datetime.now()

    def is_closeable(self):
        return self.last_processing_time and (datetime.now() - self.last_processing_time).total_seconds() > self.gap

    def close(self):
        self.state = 'closed'
        return self.values


class CollectorWindow(Window):
    def __init__(self, fields):
        super().__init__()
        self.filled = {}  # {field: {symbol: value}}
        self.fields = fields

    def add(self, values):
        values = {k: v for k, v in values.items() if k in self.fields}  # remove extra fields
        self.filled = update_dic(self.filled, values)

    def is_closeable(self):
        return set(self.filled.keys()) == set(self.fields)

    def close(self):
        self.state = 'closed'
        return self.filled


def test():
    collector = CollectorWindow(fields=['a', 'b'])

    print(collector.add({'a': 1}))
    print(collector.is_closeable(), collector.state)
    print(collector.add({'b': 1}))
    print(collector.is_closeable(), collector.state)
    print(collector.close())
    print(collector.is_closeable(), collector.state)

    import time
    session = ProcessingSessionWindow(2)
    session.add('asdf')
    print(session.is_closeable())
    time.sleep(1)
    session.add('asdf')
    print(session.is_closeable())
    time.sleep(1)
    print(session.is_closeable())
    time.sleep(1)
    print(session.is_closeable())
    print(session.close())

if __name__ == '__main__':
    test()