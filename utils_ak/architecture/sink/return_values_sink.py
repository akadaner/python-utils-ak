class ReturnValuesSink:
    """
    I'm a sink that allows to collect return values from multiple runnables.
    I'm thread-safe unless you call the same pipeline multiple times.
    """

    def __init__(self):
        super().__init__()
        self.results = {}

    def _id(self, callable):
        return id(callable)

    def store(self, mark, result):
        self.results[mark] = result

    def get_pipeline(
        self, mark
    ):  # I give the pipeline. Run it like pipeline("value") to store the value
        def collector(result):
            self.store(mark, result)

        return collector

    def get_results(self):
        return self.results


if __name__ == "__main__":

    def go():
        return "kek"

    sink = ReturnValuesSink()
    pipeline = sink.get_pipeline(
        "for_example_i_store_the_go_return_value_as_this_string"
    )

    pipeline(go())

    print(sink.get_results())
