import time

from lessmore.clock import *


@clockify("time")
def _test_time():
    time.sleep(1)


@clockify("time_with_exception")
def _test_time_with_exception():
    time.sleep(1)
    raise Exception("test")


# todo: make proper test with asserts
def test_clock():
    # usage 1
    clock = Clock()

    _test_time()
    try:
        _test_time_with_exception()
    except:
        pass
    print(clock.stats())
    clock.reset()

    # start clock
    clock.clock("first")
    time.sleep(0.1)
    # stop clock
    clock.clock("first")

    clock.clock("second")
    time.sleep(0.2)
    clock.clock("second")

    # this will add statistics to the 'first' key
    clock.clock("first")
    time.sleep(0.1)
    clock.clock("first")

    print(clock.stats())

    # usage 2
    clock = Clock(auto_close=True)
    clock.clock("first")
    time.sleep(0.1)
    # this will auto-close 'first' clock
    clock.clock("second")
    time.sleep(0.2)

    # this stops all clocks
    clock.stop()
    print("Auto close")
    print(clock.stats())

    clock = Clock()
    # trie example
    clock.reset()
    clock.clock("a")

    for i in range(10):
        clock.clock("a/b")
        time.sleep(0.1)
        clock.clock("a/b")
    clock.clock("a")
    print(clock.stats())

    try:
        clock.print_trie()
        clock.print_trie(by="count")
    except:
        pass

    print(clock["a"])

    clock.save("clock.json")

    clock.reset()
    print(clock.stats())
    clock.load("clock.json")
    print(clock.stats())

    # check max observations
    clock = Clock(max_observations=3)
    for i in range(10):
        print(clock.laps.get("a"))
        clock.start("a")
        time.sleep(0.01 * i)
        clock.stop("a")

    with clock("asdf"):
        time.sleep(1)

    print(clock.stats())


if __name__ == "__main__":
    test_clock()
