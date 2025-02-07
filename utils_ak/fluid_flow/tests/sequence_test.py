from utils_ak.fluid_flow import *


def test_flow_sequence_1():
    c1 = Container("Input", value=100, max_pressures=[None, 10])

    sequence = Sequence(
        "Sequence",
        containers=[Container(str(i), max_pressures=[5, None]) for i in range(10)],
    )

    c2 = Container("Ouput", max_pressures=[None, None])

    pipe_connect(c1, sequence)
    pipe_connect(sequence, c2)
    flow = FluidFlow(c1)
    run_fluid_flow(flow)


def test_flow_sequence_2():
    c1 = Container("Input", value=100, max_pressures=[None, 10])

    sequence = Sequence(
        "Sequence",
        containers=[Container(str(i), max_pressures=[2 - i, None]) for i in range(2)],
    )

    c2 = Container("Ouput", max_pressures=[None, None])

    pipe_connect(c1, sequence)
    pipe_connect(sequence, c2)
    flow = FluidFlow(c1)
    run_fluid_flow(flow)


if __name__ == "__main__":
    # test_flow_sequence_1()
    test_flow_sequence_2()
