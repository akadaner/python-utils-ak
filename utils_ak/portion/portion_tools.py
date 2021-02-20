import portion


def cast_interval(a, b):
    return portion.closedopen(a, b)


def calc_interval_length(interval):
    if interval.empty:
        return 0
    return sum([c.upper - c.lower for c in interval])


if __name__ == "__main__":
    interval = cast_interval(1, 3)
    print(calc_interval_length(interval))
