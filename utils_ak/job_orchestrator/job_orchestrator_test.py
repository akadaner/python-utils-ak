import time
import multiprocessing

from mongoengine import connect as connect_to_mongodb

from utils_ak.simple_microservice import run_listener_async
from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout

from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.models import *
from utils_ak.job_orchestrator.monitor_test import run_monitor
from utils_ak.job_orchestrator.config import load_config


def create_new_job(payload):
    load_config("test")
    from utils_ak.job_orchestrator.config import CONFIG

    connect_to_mongodb(host="")
    configure_loguru_stdout("DEBUG")
    logger.info("Connected to mongodb")
    time.sleep(2)
    logger.debug("Creating new job...")
    Job.drop_collection()
    Worker.drop_collection()

    payload = dict(payload)
    payload.update(
        {
            "message_broker": CONFIG["worker_message_broker"],
        }
    )

    job = Job(
        type="test",
        payload=payload,
        runnable={
            "image": "akadaner/test-worker",
            "main_file_path": r"C:\Users\Mi\Desktop\master\code\git\python-utils-ak\utils_ak\job_orchestrator\worker\test\main.py",
        },
        running_timeout=10,
    )
    job.save()


def test_job_orchestrator(payload=None):
    CONFIG = load_config("test")
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(host=CONFIG["mongodb_host"], db=CONFIG["mongodb_db"])
    logger.info("Connected to mongodb")
    controller = ProcessController()
    run_listener_async("job_orchestrator", message_broker=CONFIG["message_broker"])
    job_orchestrator = JobOrchestrator(controller, CONFIG["message_broker"])
    multiprocessing.Process(target=run_monitor).start()
    if payload:
        multiprocessing.Process(target=create_new_job, args=(payload,)).start()
    job_orchestrator.run()


def test_success():
    test_job_orchestrator({"type": "batch"})


def test_stalled():
    test_job_orchestrator({"type": "batch", "stalled_timeout": 600})


def test_timeout():
    test_job_orchestrator({"type": "batch", "running_timeout": 20})


def test_failure():
    test_job_orchestrator({"type": "batch", "initializing_timeout": 600})


def test_run():
    test_job_orchestrator()


if __name__ == "__main__":
    # test_success()
    # test_stalled()
    # test_timeout()
    # test_failure()
    test_run()
