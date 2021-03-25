import time
import multiprocessing

from mongoengine import connect as connect_to_mongodb

from utils_ak.simple_microservice import run_listener_async
from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout

from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.models import *

from utils_ak.job_orchestrator.config import settings
from utils_ak.job_orchestrator.tests.test_monitor import run_monitor

MESSAGE_BROKER = settings.as_dict()["TRANSPORT"]["message_broker"]


def create_new_job(payload):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(
        host=settings.job_queue.mongodb_host, db=settings.job_queue.mongodb_db
    )
    logger.info("Connected to mongodb")
    time.sleep(2)
    logger.debug("Creating new job...")
    Job.drop_collection()
    Worker.drop_collection()

    payload = dict(payload)
    payload.update(
        {
            "message_broker": MESSAGE_BROKER,
        }
    )

    job = Job(
        type="test",
        payload=payload,
        runnable={
            "image": "akadaner/test-worker",
            "python_main": "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/python-utils-ak/utils_ak/job_orchestrator/worker/test/main.py",
        },
        running_timeout=15,
    )
    job.save()


def test_job_orchestrator(payload=None):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(
        host=settings.job_queue.mongodb_host, db=settings.job_queue.mongodb_db
    )
    logger.info("Connected to mongodb")

    controller = ProcessController()

    run_listener_async("job_orchestrator", message_broker=MESSAGE_BROKER)
    job_orchestrator = JobOrchestrator(controller, MESSAGE_BROKER)
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
