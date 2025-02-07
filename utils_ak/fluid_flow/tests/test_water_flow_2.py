def test_water_flow_2():
    n_boilings = 2
    packing1_1 = Processor(
        "Packing1",
        items=["a", "a1"],
        max_pressures=[200, None],
        processing_time=0,
        limits=[750, None],
    )
    packing2_1 = Processor(
        "Packing2",
        items=["b", "b1"],
        max_pressures=[400, None],
        processing_time=0,
        limits=[100, None],
    )
    packing1_2 = Processor(
        "Packing1",
        items=["a", "a1"],
        max_pressures=[200, None],
        processing_time=0,
        limits=[750, None],
    )
    packing2_2 = Processor(
        "Packing2",
        items=["b", "b1"],
        max_pressures=[400, None],
        processing_time=0,
        limits=[100, None],
    )
    packing_queue1 = Queue("PackingQueue1", [packing1_1, packing2_1, packing1_2, packing2_2])

    packing3_1 = Processor(
        "Packing3",
        items=["b", "b2"],
        max_pressures=[200, None],
        processing_time=0,
        limits=[150, None],
    )
    packing3_2 = Processor(
        "Packing3",
        items=["b", "b2"],
        max_pressures=[200, None],
        processing_time=0,
        limits=[150, None],
    )
    packing_queue2 = Queue("PackingQueue1", [packing3_1, packing3_2])

    for _ in range(n_boilings):
        drenator = Container("Drenator", value=1000, max_pressures=[None, None])

        melting1 = Processor(
            "Melting1",
            items=["a", "a"],
            max_pressures=[1000, 1000],
            processing_time=0,
            limits=[750, 750],
        )
        melting2 = Processor(
            "Melting2",
            items=["b", "b"],
            max_pressures=[1000, 1000],
            processing_time=0,
            limits=[250, 250],
        )
        melting_queue = Queue("MeltingQueue", [melting1, melting2], break_funcs={"in": lambda old, new: 1})

        cooling1 = Processor(
            "Cooling1",
            items=["a", "a"],
            max_pressures=[None, None],
            processing_time=0.5,
            limits=[750, 750],
        )
        cooling2 = Processor(
            "Cooling2",
            items=["b", "b"],
            max_pressures=[None, None],
            processing_time=0.5,
            limits=[250, 250],
        )
        cooling_queue = Queue("CoolingQueue", [cooling1, cooling2])

        packing_hub = Hub("Hub")

        pipe_connect(drenator, melting_queue, "drenator-melting")
        pipe_connect(melting_queue, cooling_queue, "melting-cooling")
        pipe_connect(cooling_queue, packing_hub, "cooling-hub")

        pipe_connect(packing_hub, packing_queue1, "hub-packing_queue1")
        pipe_connect(packing_hub, packing_queue2, "hub-packing_queue2")

        flow = FluidFlow(drenator)
        run_fluid_flow(flow)

        maker, make = init_block_maker("root", axis=1)

        for node in drenator.iterate("down"):
            if node.active_periods():
                for period in node.active_periods():
                    label = "-".join([str(node.name), period[0]])
                    beg, end = period[1:]
                    beg, end = (
                        custom_round(beg * 60, 5) // 5,
                        custom_round(end * 60, 5) // 5,
                    )
                    make(label, x=[beg, 0], size=(end - beg, 1))
        print(maker.root.tabular())

        # clean-up
        for node in drenator.iterate("down"):
            node.reset()

        # remove packers from current boiling
        pipe_disconnect(packing_hub, packing_queue1)
        pipe_disconnect(packing_hub, packing_queue2)


if __name__ == "__main__":
    test_water_line_flow_1()
    # test_water_line_flow_2_multiple_boilings()
