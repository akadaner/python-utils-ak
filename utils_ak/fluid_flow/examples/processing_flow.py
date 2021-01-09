from utils_ak.fluid_flow import *

def gen_processing_flow():
    container1 = Container('Input', max_pressure_out=50)
    container1.value = 100
    container2 = ProcessingContainer('Ouput')
    EVENT_MANAGER.subscribe('processing_container.set_pressure', container2.on_set_pressure)

    cable = Cable('Cable')
    connect(container1, cable)
    connect(cable, container2)
    return FluidFlow(container1)


def test_processing_flow():
    import warnings
    warnings.filterwarnings("ignore")

    flow = gen_processing_flow()
    run_flow(flow)

if __name__ == '__main__':
    test_processing_flow()