from utils_ak.config import cast_config

import os

BASE_DIR = __file__
for i in range(2):
    BASE_DIR = os.path.dirname(BASE_DIR)

CONFIG = None


def load_config(environment):
    global CONFIG
    CONFIG = cast_config(os.path.join(BASE_DIR, "config", f"{environment}.yml"))
    return CONFIG


def test():
    print(CONFIG)
    load_config("test")
    print(CONFIG)


if __name__ == "__main__":
    test()
