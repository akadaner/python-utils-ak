from utils_ak.fluid_flow import *
import warnings

warnings.filterwarnings("ignore")


def test_flow_processor_1():
    container = Container("Input", value=100, max_pressures=[None, 10])

    processor = Processor("Output")

    pipe_connect(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_2():
    container = Container("Input", value=100)
    processor = Processor(
        "Output", processing_time=5, max_pressures=[10, None], transformation_factor=2.0
    )
    pipe_connect(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_zero_pressure():
    container = Container("Input", value=100)
    processor = Processor("Output", max_pressures=[0, 0])
    pipe_connect(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


def test_flow_processor_limit():
    container = Container("Input", value=100, max_pressures=[None, 10])
    processor = Processor("Output", limits=[50, None])
    pipe_connect(container, processor)
    flow = FluidFlow(container, verbose=True)
    run_flow(flow)


if __name__ == "__main__":
    test_flow_processor_1()
    test_flow_processor_2()
    test_flow_processor_zero_pressure()
    test_flow_processor_limit()
