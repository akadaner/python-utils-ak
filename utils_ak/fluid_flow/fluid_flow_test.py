from utils_ak.fluid_flow import *
from utils_ak.log import *
import warnings
warnings.filterwarnings("ignore")


def test_pipe_switch_1():
    ci1 = Container('I1')
    ci2 = Container('I2')
    co1 = Container('O1')
    co2 = Container('O2')

    pipe_together(ci1, co1, '1')
    pipe_together(ci2, co2, '2')

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(co1, co2, 'in')

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(co1, co2, 'in')
    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()

    pipe_switch(ci1, ci2, 'out')

    for node in ci1.iterate():
        print(node)
    for node in ci2.iterate():
        print(node)
    print()


def test_pipe_switch_2():
    ci1 = Container('I1')
    co1 = Container('O1')
    co2 = Container('O2')

    pipe_together(ci1, co1)

    for node in ci1.iterate():
        print(node)
    print()
    pipe_switch(co1, co2, 'in')

    for node in ci1.iterate():
        print(node)
    print()



def test_flow_container_1():
    container1 = Container('Input', max_pressures=[None, 50])
    container1.value = 100
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_2():
    container1 = Container('Input', max_pressures=[None, 50], limits=[None, 30])
    container1.value = 100
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_3():
    container1 = Container('Input', max_pressures=[None, 50], limits=[None, 30])
    container1.value = 100
    container2 = Container('Output', max_pressures=[5, None], limits=[20, None])

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_processor_1():
    container = Container('Input', max_pressures=[None, 10])
    container.value = 100

    processor = Processor('Output')

    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_2():
    container = Container('Input')
    container.value = 100
    processor = Processor('Output', processing_time=5, max_pressures=[10, None], transformation_factor=2.)
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_zero_pressure():
    container = Container('Input')
    container.value = 100
    processor = Processor('Output', max_pressures=[0, 0])
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_limit():
    container = Container('Input', max_pressures=[None, 10])
    container.value = 100
    processor = Processor('Output', limits=[50, None])
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_hub_1():
    parent = Container('Parent', max_pressures=[None, 20])
    parent.value = 100

    hub = Hub('Hub')

    child1 = Container('Child1', max_pressures=[15, None])
    child2 = Container('Child2', max_pressures=[10, None])

    pipe_together(parent, hub, 'parent-hub')
    pipe_together(hub, child1, 'hub-child1')
    pipe_together(hub, child2, 'hub-child2')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


def test_flow_hub_2():
    parent = Container('Parent', max_pressures=[None, 20])
    parent.value = 100

    hub = Hub('Hub')

    child1 = Processor('Child1', max_pressures=[15, None], limits=[30, None])
    child2 = Processor('Child2', max_pressures=[10, None])

    pipe_together(parent, hub, 'parent-hub')
    pipe_together(hub, child1, 'hub-child1')
    pipe_together(hub, child2, 'hub-child2')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


def test_flow_queue_1_in():
    parent = Container('Parent', max_pressures=[None, 20])
    parent.value = 100

    child1 = Container('Child1', max_pressures=[20, None], limits=[40, None])
    child2 = Container('Child2', max_pressures=[10, None], limits=[50, None])

    queue = Queue('Queue', [child1, child2])

    pipe_together(parent, queue, 'parent-queue')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


def test_flow_queue_2_out():
    parent1 = Container('Parent1', value=100, max_pressures=[None, 10], limits=[None, 100])
    parent2 = Container('Parent2', value=100, max_pressures=[None, 20], limits=[None, 100])
    queue = Queue('Parent', [parent1, parent2])

    child = Container('Child', max_pressures=[None, None])
    pipe_together(queue, child, 'parent-queue')

    flow = FluidFlow(queue, verbose=True)
    run_flow(flow)


def test_flow_queue_3():
    parent = Container('Parent', max_pressures=[None, 20])
    parent.value = 100

    child1 = Processor('Child1', max_pressures=[20, None], processing_time=5, limits=[40, None])
    child2 = Processor('Child2', max_pressures=[10, None], processing_time=5, limits=[50, None])

    queue = Queue('Queue', [child1, child2])

    pipe_together(parent, queue, 'parent-queue')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    configure_logging(stream_level=logging.INFO)
    # test_pipe_switch_1()
    # test_pipe_switch_2()
    # test_flow_container_1()
    # test_flow_container_2()
    # test_flow_container_3()
    # test_flow_processor_1()
    # test_flow_processor_2()
    # test_flow_processor_zero_pressure()
    # test_flow_processor_limit()
    # test_flow_hub_1()
    # test_flow_hub_2()
    # test_flow_queue_1_in()
    # test_flow_queue_2_out()
    test_flow_queue_3()