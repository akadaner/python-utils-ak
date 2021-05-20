from loguru import logger

from utils_ak.deployment import *

from utils_ak.job_orchestrator.worker.worker_tests import (
    _test_microservice_worker,
    _test_microservice_worker_deployment,
)
from utils_ak.job_orchestrator.worker.sample_worker.sample_worker import *
from utils_ak.job_orchestrator.tests.config.config import config
from utils_ak.job_orchestrator.worker.sample_worker.main import MAIN


def run_batch():
    logger.info("Running batch SampleWorker")
    _test_microservice_worker(
        SampleWorker,
        {"type": "batch", "message_broker": config.TRANSPORT},
        run_listener=True,
    )


def run_streaming():
    logger.info("Running streaming SampleWorker")

    _test_microservice_worker(
        SampleWorker,
        {"type": "streaming", "message_broker": config.TRANSPORT},
        run_listener=True,
    )


def run_deployment():
    logger.info("Running batch SampleWorker using deployment via ProcessController")

    controller = ProcessController()
    _test_microservice_worker_deployment(
        {"type": "batch", "message_broker": config.TRANSPORT},
        MAIN,
        controller,
    )


if __name__ == "__main__":
    run_batch()
    # run_streaming()
    # run_deployment()
