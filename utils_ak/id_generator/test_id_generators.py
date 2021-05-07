from utils_ak.id_generator.id_generators import *


def test():
    for generator in [UUIDGenerator(), IncrementalIDGenerator()]:
        for i in range(5):
            print(i, generator.gen_id())


if __name__ == "__main__":
    test()
