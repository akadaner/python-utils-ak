import time
from loguru import logger

from utils_ak.loguru import configure_loguru_stdout
from utils_ak.simple_microservice import run_listener_async
from utils_ak.job_orchestrator.worker.worker import run_worker
from utils_ak.job_orchestrator.worker.gen_deployment import create_deployment
from utils_ak.deployment import *


def _test_microservice_worker(worker_cls, payload, run_listener=True):
    configure_loguru_stdout("DEBUG")
    if run_listener:
        run_listener_async("monitor_in", message_broker=payload["message_broker"])
    time.sleep(2)
    run_worker(worker_cls, {"worker_id": "worker_id", "payload": payload})
    logger.info("Finished")


def _test_microservice_worker_deployment(
    payload,
    python_main,
    controller,
    run_listener=True,
):
    configure_loguru_stdout("DEBUG")

    deployment = create_deployment(
        "worker", "<deployment_id>", payload, python_main=python_main
    )

    if run_listener:
        run_listener_async("monitor_in", message_broker=payload["message_broker"])

    controller.stop(deployment["id"])
    controller.start(deployment)
    time.sleep(5)
    controller.stop(deployment["id"])
