import portion
from utils_ak.numeric import is_numeric

class PortionInvervalWrapper:
    def __init__(self, a, b):
        self.interval = portion.closedopen(a, b)

    def length(self):
        if self.interval.empty:
            return 0
        return sum([c.upper - c.lower for c in self.interval])


def cast_interval(a, b=None):
    if isinstance(a, PortionInvervalWrapper):
        return a
    elif isinstance(a, portion.Interval):
        interval = PortionInvervalWrapper(0, 0)
        interval.interval = a
        return interval
    elif is_numeric(a) and is_numeric(b):
        return PortionInvervalWrapper(a, b)
    else:
        raise Exception('Unknown interval type')


if __name__ == '__main__':
    interval = cast_interval(1, 3)
    print(interval.length())
