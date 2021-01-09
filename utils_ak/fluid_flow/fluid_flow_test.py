from utils_ak.fluid_flow import *
from utils_ak.log import *
import warnings
warnings.filterwarnings("ignore")


def test_flow_1():
    container1 = Container('Input', max_pressure_out=50)
    container1.value = 100
    container2 = Container('Ouput')
    pipe = Pipe('Pipe')

    connect(container1, pipe)
    connect(pipe, container2)
    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_2():
    container1 = Container('Input', max_pressure_out=50)
    container1.value = 100
    container2 = Processor('Ouput')

    pipe = Pipe('Pipe')
    connect(container1, pipe)
    connect(pipe, container2)
    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_3():
    container1 = Container('Input')
    container1.value = 100
    container2 = Processor('Ouput', processing_time=5, max_pressure_in=10, transformation_factor=2.)

    pipe = Pipe('Pipe')
    connect(container1, pipe)
    connect(pipe, container2)
    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


def test_flow_4():
    container1 = Container('Input')
    container1.value = 100
    container2 = Processor('Ouput', max_pressure_in=0)

    pipe = Pipe('Pipe')
    connect(container1, pipe)
    connect(pipe, container2)
    flow = FluidFlow(container1, verbose=True)
    run_flow(flow)


if __name__ == '__main__':
    configure_logging(stream_level=logging.INFO)
    test_flow_1()
    test_flow_2()
    test_flow_3()
    test_flow_4()
