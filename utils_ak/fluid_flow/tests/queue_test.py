from utils_ak.fluid_flow import *


def test_flow_queue_1_in():
    parent = Container('Parent', value=100, max_pressures=[None, 20])

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
    parent = Container('Parent', value=100, max_pressures=[None, 20])

    child1 = Processor('Child1', max_pressures=[20, None], processing_time=5, limits=[40, None])
    child2 = Processor('Child2', max_pressures=[10, None], processing_time=5, limits=[50, None])

    queue = Queue('Queue', [child1, child2])

    pipe_together(parent, queue, 'parent-queue')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)

if __name__ == '__main__':
    test_flow_queue_1_in()
    test_flow_queue_2_out()
    test_flow_queue_3()