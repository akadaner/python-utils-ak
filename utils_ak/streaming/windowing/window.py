from datetime import datetime, timedelta
from utils_ak.builtin import update_dic
from utils_ak.time import *
from sortedcollections import SortedList
from utils_ak.numeric import custom_round


class Window:
    def __init__(self):
        self.state = "open"  # 'open', 'closed'

    def close(self):
        self.state = "closed"

    def is_emitable(self):
        raise NotImplementedError

    def is_closeable(self):
        raise NotImplementedError

    def emit(self):
        raise NotImplementedError


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
        return (
            self.last_processing_time
            and (datetime.now() - self.last_processing_time).total_seconds() > self.gap
        )

    def close(self):
        self.state = "closed"
        return self.values


class FieldsCollectorWindow(Window):
    def __init__(self, fields):
        super().__init__()
        self.filled = {}  # {field: {symbol: value}}
        self.fields = fields

    def add(self, values):
        values = {
            k: v for k, v in values.items() if k in self.fields
        }  # remove extra fields
        self.filled = update_dic(self.filled, values)

    def is_closeable(self):
        return set(self.filled.keys()) == set(self.fields)

    def close(self):
        self.state = "closed"
        return self.filled


class CollectorWindow(Window):
    def __init__(
        self, timestamp_extractor=lambda msg: msg["values"][0], window_size=300
    ):
        super().__init__()
        self.timestamp_extractor = timestamp_extractor
        self.buffer = SortedList(key=timestamp_extractor)
        self.window_size = window_size

        self.cur_ts = None
        self.last_ts = None

    def add(self, values):
        if self.cur_ts is None:
            self.cur_ts = custom_round(
                self.timestamp_extractor(values[0]), self.window_size, "floor"
            )

        for value in values:
            self.buffer.add(value)

        self.last_ts = self.timestamp_extractor(values[-1])

    def is_emitable(self):
        if self.cur_ts is None:
            return False
        return (self.last_ts - self.cur_ts) >= self.window_size

    def emit(self):
        next_ts = self.cur_ts + self.window_size
        index = self.buffer.bisect_left({"values": [next_ts]})
        values = self.buffer[:index]
        del self.buffer[:index]
        self.cur_ts = next_ts
        return values


def test():
    window = FieldsCollectorWindow(fields=["a", "b"])

    print(window.add({"a": 1}))
    print(window.is_closeable(), window.state)
    print(window.add({"b": 1}))
    print(window.is_closeable(), window.state)
    print(window.close())
    print(window.is_closeable(), window.state)

    import time

    window = ProcessingSessionWindow(2)
    window.add("asdf")
    print(window.is_closeable())
    time.sleep(1)
    window.add("asdf")
    print(window.is_closeable())
    time.sleep(1)
    print(window.is_closeable())
    time.sleep(1)
    print(window.is_closeable())
    print(window.close())


def test_collector_window():
    message = {"source": "binance_timebars_1800", "values": [1617724522.346, 1, 2, 3]}
    messages = [
        {
            "source": "binance_timebars_1800",
            "values": [i * 60, 1, 2, 3],
        }
        for i in range(10)
    ]

    window = CollectorWindow()
    for message in messages:
        print("Adding message", message)
        window.add([message])
        print(window.buffer)
        print(window.is_emitable())
        if window.is_emitable():
            print(window.emit())
            print(window.buffer)


if __name__ == "__main__":
    # test()

    test_collector_window()
