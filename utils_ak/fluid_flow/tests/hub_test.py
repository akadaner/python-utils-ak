from utils_ak.fluid_flow import *



def test_flow_hub_1():
    parent = Container('Parent', value=100, max_pressures=[None, 20])

    hub = Hub('Hub')

    child1 = Container('Child1', max_pressures=[15, None])
    child2 = Container('Child2', max_pressures=[10, None])

    pipe_together(parent, hub, 'parent-hub')
    pipe_together(hub, child1, 'hub-child1')
    pipe_together(hub, child2, 'hub-child2')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


def test_flow_hub_2():
    parent = Container('Parent', value=100, max_pressures=[None, 20])

    hub = Hub('Hub')

    child1 = Processor('Child1', max_pressures=[15, None], limits=[30, None])
    child2 = Processor('Child2', max_pressures=[10, None])

    pipe_together(parent, hub, 'parent-hub')
    pipe_together(hub, child1, 'hub-child1')
    pipe_together(hub, child2, 'hub-child2')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


def test_flow_hub_3():
    parent = Container('Parent', item='a', value=100, max_pressures=[None, 20])

    hub = Hub('Hub')

    child1 = Container('Child1', item='b', max_pressures=[15, None])
    child2 = Container('Child2', item='a', max_pressures=[10, None])

    pipe_together(parent, hub, 'parent-hub')
    pipe_together(hub, child1, 'hub-child1')
    pipe_together(hub, child2, 'hub-child2')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    # test_flow_hub_1()
    # test_flow_hub_2()
    test_flow_hub_3()