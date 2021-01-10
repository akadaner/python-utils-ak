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
    container1 = Container('Input', max_pressures=[0, 50])
    container1.value = 100
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_2():
    container1 = Container('Input', max_pressures=[0, 50], limits=[None, 30])
    container1.value = 100
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_container_3():
    container1 = Container('Input', max_pressures=[0, 50], limits=[None, 30])
    container1.value = 100
    container2 = Container('Output', max_pressures=[5, 0], limits=[20, 0])

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_processor_1():
    container = Container('Input', max_pressures=[0, 10])
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
    processor = Processor('Output', limits=[50, 0])
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_hub_1():
    container = Container('Input', max_pressure_out=20)
    container.value = 100

    hub = Hub('Hub')

    processor1 = Processor('Output1', max_pressure_in=15)
    processor2 = Processor('Output2', max_pressure_in=10)

    pipe_together(container, hub, 'container-hub')
    pipe_together(hub, processor1, 'hub-processor1')
    pipe_together(hub, processor2, 'hub-processor2')

    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_hub_2():
    container = Container('Input', max_pressure_out=20)
    container.value = 100

    hub = Hub('Hub')

    processor1 = Processor('Output1', max_pressure_in=15, processing_limit=30)
    processor2 = Processor('Output2', max_pressure_in=10)

    pipe_together(container, hub, 'container-hub')
    pipe_together(hub, processor1, 'hub-processor1')
    pipe_together(hub, processor2, 'hub-processor2')

    flow = FluidFlow(container, verbose=True)
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
    test_flow_processor_limit()