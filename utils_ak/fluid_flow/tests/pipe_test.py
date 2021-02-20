from utils_ak.fluid_flow import *


def test_pipe_switch_1():
    from utils_ak.fluid_flow.actors.container import Container

    ci1 = Container("I1")
    ci2 = Container("I2")
    co1 = Container("O1")
    co2 = Container("O2")

    pipe_connect(ci1, co1, "1")
    pipe_connect(ci2, co2, "2")

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(co1, co2, "in")

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(co1, co2, "in")
    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(ci1, ci2, "out")

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()


def test_pipe_switch_2():
    from utils_ak.fluid_flow.actors.container import Container

    ci1 = Container("I1")
    co1 = Container("O1")
    co2 = Container("O2")

    pipe_connect(ci1, co1)

    for node in ci1.iterate():
        print(node)
    print()
    pipe_switch(co1, co2, "in")

    for node in ci1.iterate():
        print(node)
    print()


if __name__ == "__main__":
    test_pipe_switch_1()
    test_pipe_switch_2()
