import portion


class PortionInvervalWrapper:
    def __init__(self, a, b):
        self.interval = portion.closedopen(a, b)

    def length(self):
        if self.interval.empty:
            return 0
        return sum([c.upper - c.lower for c in self.interval])


if __name__ == '__main__':
    print(dir(portion))
    interval = PortionInvervalWrapper(1, 3)
    print(interval.length())
