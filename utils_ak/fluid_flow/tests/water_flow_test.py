from utils_ak.fluid_flow import *


def test_water_line_flow():
    drenator = Container('Drenator', value=1000, max_pressures=[None, None])

    melting1 = Processor('Melting1', max_pressures=[1000, None], processing_time=0, limits=[1000, None])
    melting_queue = Queue('MeltingQueue', [melting1])

    cooling1 = Processor('Cooling1', max_pressures=[1000, None], processing_time=1, limits=[1000, None])
    cooling_queue = Queue('CoolingQueue', [cooling1])

    packing_hub = Hub('Hub')

    packing1 = Processor('Packing1', max_pressures=[200, None], processing_time=0, limits=[1000, None])
    packing_queue1 = Queue('PackingQueue1', [packing1])

    pipe_together(drenator, melting_queue, 'drenator-melting')
    pipe_together(melting_queue, cooling_queue, 'melting-cooling')
    pipe_together(cooling_queue, packing_hub, 'cooling-hub')

    pipe_together(packing_hub, packing_queue1, 'hub-packing_queue1')

    flow = FluidFlow(drenator, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    test_water_line_flow()