import time
import os
from utils_ak.loguru import logger, configure_loguru_stdout
from utils_ak.deployment.io import *


def _test_controller(controller_cls):
    configure_loguru_stdout("DEBUG")

    deployment_fn = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "examples/hello_world/deployment.yml",
    )
    deployment = read_deployment(deployment_fn)
    ctrl = controller_cls()
    logger.info("Starting")
    ctrl.start(deployment)
    time.sleep(3)
    logger.info("Logs")
    ctrl.log(deployment["id"])
    logger.info("Stopping")
    ctrl.stop(deployment["id"])
