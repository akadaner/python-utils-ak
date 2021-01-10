from utils_ak.fluid_flow import *
from utils_ak.block_tree import *

def test_water_line_flow():
    drenator = Container('Drenator', value=1000, max_pressures=[None, None])

    melting1 = Processor('Melting1', max_pressures=[1000, None], processing_time=0, limits=[750, 750])
    melting2 = Processor('Melting2', max_pressures=[1000, None], processing_time=0, limits=[250, 250])
    melting_queue = Queue('MeltingQueue', [melting1, melting2])

    # cooling1 = Processor('Cooling1', max_pressures=[1000, None], processing_time=1, limits=[1000, 1000])
    cooling1 = Processor('Cooling1', max_pressures=[750, None], processing_time=1, limits=[750, 750])
    cooling2 = Processor('Cooling2', max_pressures=[250, None], processing_time=1, limits=[250, 250])
    cooling_queue = Queue('CoolingQueue', [cooling1, cooling2])

    packing_hub = Hub('Hub')

    packing1 = Processor('Packing1', max_pressures=[200, None], processing_time=0, limits=[1000, None])
    packing_queue1 = Queue('PackingQueue1', [packing1])

    pipe_together(drenator, melting_queue, 'drenator-melting')
    pipe_together(melting_queue, cooling_queue, 'melting-cooling')
    pipe_together(cooling_queue, packing_hub, 'cooling-hub')

    pipe_together(packing_hub, packing_queue1, 'hub-packing_queue1')

    flow = FluidFlow(drenator, verbose=True)
    run_flow(flow)

    maker, make = init_block_maker('root', axis=1)

    for node in drenator.iterate('down'):
        if node.active_periods():
            for period in node.active_periods():
                label = '-'.join([str(node.id), period[0]])
                beg, end = period[1:]
                beg, end = custom_round(beg * 60, 5) // 5, custom_round(end * 60, 5) // 5
                make(label, x=[beg, 0], size=(end - beg, 1))
    print(maker.root.tabular())


if __name__ == '__main__':
    test_water_line_flow()