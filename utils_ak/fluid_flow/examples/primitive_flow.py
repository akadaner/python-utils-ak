from utils_ak.fluid_flow import *


def test_primitive_flow():
    import warnings
    warnings.filterwarnings("ignore")

    container1 = Container('Input', max_pressure_out=50)
    container1.value = 100
    container2 = Container('Ouput')
    cable = Cable('Cable')

    connect(container1, cable)
    connect(cable, container2)

    flow = FluidFlow(container1)
    EVENT_MANAGER.subscribe('', flow.update)
    EVENT_MANAGER.add_event('update', 0, {})
    EVENT_MANAGER.run()

if __name__ == '__main__':
    test_primitive_flow()