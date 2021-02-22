import anyconfig
import time
from utils_ak.loguru import logger, configure_loguru_stdout


def test_controller(controller_cls):
    configure_loguru_stdout("DEBUG")

    deployment = anyconfig.load("../../example/deployment.yml")
    ctrl = controller_cls()
    logger.info("Starting")
    ctrl.start(deployment)
    time.sleep(3)
    logger.info("Logs")
    ctrl.log(deployment["id"])
    logger.info("Stopping")
    ctrl.stop(deployment["id"])
