from utils_ak.fluid_flow import *


def test_flow_container_1():
    container1 = Container('Input', value=100, max_pressures=[None, 50])
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_2():
    container1 = Container('Input', value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_3():
    container1 = Container('Input', value=100, max_pressures=[None, 50], limits=[None, 30])
    container2 = Container('Output', max_pressures=[5, None], limits=[20, None])

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    test_flow_container_1()
    # test_flow_container_2()
    # test_flow_container_3()
