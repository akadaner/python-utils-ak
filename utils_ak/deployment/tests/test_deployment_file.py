from utils_ak.deployment.deployment_file import *


def test_read_deployment_file():
    deployment = read_deployment("../examples/hello_world/deployment.yml")
    print(deployment)


if __name__ == "__main__":
    test_read_deployment_file()
