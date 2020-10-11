""" Pipeline is a callable, which runs consecutively an array of callables. """
class Pipeline(object):
    def __init__(self, array):
        self.array = array

        assert len(self.array) > 1

    def run(self, *args, **kwargs):
        res = self.array[0](*args, **kwargs)
        for pipe in self.array[1:]:
            res = pipe(res)
        return res

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)


if __name__ == '__main__':
    print(Pipeline([lambda y: y * 2, lambda x: x * 2, lambda x: x + 10]).run(15))
