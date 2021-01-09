from utils_ak.fluid_flow import *
from utils_ak.log import *
import warnings
warnings.filterwarnings("ignore")


def test_flow_1():
    container1 = Container('Input', max_pressure_out=50)
    container1.value = 100
    container2 = Container('Output')

    pipe_together(container1, container2)

    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_2():
    container = Container('Input', max_pressure_out=50)
    container.value = 100
    processor = Processor('Output')
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_3():
    container = Container('Input')
    container.value = 100
    processor = Processor('Output', processing_time=5, max_pressure_in=10, transformation_factor=2.)
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_4_zero_pressure():
    container = Container('Input')
    container.value = 100
    processor = Processor('Output', max_pressure_in=0)
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_5_processing_limit():
    container = Container('Input', max_pressure_out=10)
    container.value = 100
    processor = Processor('Output', processing_limit=50)
    pipe_together(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_6_hub():
    container = Container('Input', max_pressure_out=20)
    container.value = 100

    hub = Hub('Hub')

    processor1 = Processor('Output1')
    processor2 = Processor('Output2')

    pipe_together(container, hub)
    pipe_together(hub, processor1)
    pipe_together(hub, processor2)

    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    configure_logging(stream_level=logging.INFO)
    # test_flow_1()
    # test_flow_2()
    # test_flow_3()
    # test_flow_4_zero_pressure()
    # test_flow_5_processing_limit()
    test_flow_6_hub()

