from utils_ak.fluid_flow import *


def test_water_line_flow():
    parent = Container('Drenator', max_pressures=[None, 20])

    child1 = Processor('Child1', max_pressures=[20, None], processing_time=5, limits=[40, None])
    child2 = Processor('Child2', max_pressures=[10, None], processing_time=5, limits=[50, None])

    queue = Queue('Queue', [child1, child2])

    pipe_together(parent, queue, 'parent-queue')

    flow = FluidFlow(parent, verbose=True)
    run_flow(flow)

