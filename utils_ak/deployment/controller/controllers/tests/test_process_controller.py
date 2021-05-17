from utils_ak.deployment.controller.controller_tests import _test_controller
from utils_ak.deployment.controller.controllers.process_controller import (
    ProcessController,
)


def test_process_controller():
    _test_controller(ProcessController)


if __name__ == "__main__":
    test_process_controller()
