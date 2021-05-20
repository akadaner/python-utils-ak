from utils_ak.deployment import *

from utils_ak.job_orchestrator.worker.worker_tests import (
    _test_microservice_worker,
    _test_microservice_worker_deployment,
)
from utils_ak.job_orchestrator.worker.sample_worker.sample_worker import *
from utils_ak.job_orchestrator.tests.config.config import config
from utils_ak.job_orchestrator.worker.sample_worker.main import path


def run_batch():
    _test_microservice_worker(
        SampleWorker,
        {"type": "batch", "message_broker": config.TRANSPORT},
        run_listener=True,
    )


def run_streaming():
    _test_microservice_worker(
        SampleWorker,
        {"type": "streaming", "message_broker": config.TRANSPORT},
        run_listener=True,
    )


def run_deployment():
    controller = ProcessController()
    _test_microservice_worker_deployment(
        {"type": "batch", "message_broker": config.TRANSPORT},
        path,
        controller,
    )


if __name__ == "__main__":
    run_batch()
    # run_streaming()
    # run_deployment()
