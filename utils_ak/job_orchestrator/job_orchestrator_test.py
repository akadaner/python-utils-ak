import multiprocessing
import time
import sys

from mongoengine import connect

from utils_ak.simple_microservice import run_listener_async
from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout
from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.models import *
from utils_ak.job_orchestrator.monitor_test import run_monitor
from loguru import logger

BROKER = "zmq"
BROKER_CONFIG = {
    "endpoints": {
        "monitor_in": {"endpoint": "tcp://localhost:5555", "type": "sub"},
        "monitor_out": {"endpoint": "tcp://localhost:5556", "type": "sub"},
        "job_orchestrator": {"endpoint": "tcp://localhost:5557", "type": "pub"},
    }
}

WORKER_BROKER_CONFIG = {
    "endpoints": {
        "monitor_in": {"endpoint": "tcp://host.docker.internal:5555", "type": "sub"},
        "monitor_out": {"endpoint": "tcp://host.docker.internal:5556", "type": "sub"},
        "job_orchestrator": {
            "endpoint": "tcp://host.docker.internal:5557",
            "type": "pub",
        },
    }
}


MESSAGE_BROKER = (BROKER, BROKER_CONFIG)
WORKER_MESSAGE_BROKER = (BROKER, WORKER_BROKER_CONFIG)


def create_new_job(payload):
    connect(
        host="mongodb+srv://arseniikadaner:Nash0lsapog@cluster0.2umoy.mongodb.net/feature-store?retryWrites=true&w=majority"
    )
    configure_loguru_stdout("DEBUG")
    logger.info("Connected to mongodb")
    time.sleep(2)
    logger.debug("Creating new job...")
    Job.drop_collection()
    Worker.drop_collection()

    payload = dict(payload)
    payload.update(
        {
            "message_broker": WORKER_MESSAGE_BROKER,
        }
    )

    job = Job(
        type="test",
        payload=payload,
        runnable={
            "image": "akadaner/test-worker",
            "main_file_path": r"C:\Users\Mi\Desktop\master\code\git\python-utils-ak\utils_ak\job_orchestrator\worker\test\main.py",
        },
    )
    job.save()


def test_job_orchestrator(payload):
    configure_loguru_stdout("DEBUG")
    connect(
        host="mongodb+srv://arseniikadaner:Nash0lsapog@cluster0.2umoy.mongodb.net/feature-store?retryWrites=true&w=majority"
    )
    logger.info("Connected to mongodb")
    controller = ProcessController()
    run_listener_async("job_orchestrator", message_broker=MESSAGE_BROKER)
    job_orchestrator = JobOrchestrator(controller, MESSAGE_BROKER)
    multiprocessing.Process(target=run_monitor).start()
    multiprocessing.Process(target=create_new_job, args=(payload,)).start()
    job_orchestrator.run()


def test_success():
    test_job_orchestrator({"type": "batch", "running_timeout": 600})


def test_stalled():
    test_job_orchestrator({"type": "batch", "running_timeout": 600})


def test_failure():
    test_job_orchestrator({"type": "batch", "pre_running_timeout": 600})


if __name__ == "__main__":
    test_failure()
