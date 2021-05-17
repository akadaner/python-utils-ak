import time
import multiprocessing

from mongoengine import connect as connect_to_mongodb

from utils_ak.simple_microservice import run_listener_async
from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout

from utils_ak.job_orchestrator.models import *
from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.tests.test_monitor import run_monitor

# TEST_MAIN = "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/python-utils-ak/utils_ak/job_orchestrator/worker/test/main.py"
#

def create_new_job(config, payload, python_main=TEST_MAIN):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(host=config.MONGODB_HOST, db=config.MONGODB_DB)
    logger.info("Connected to mongodb")
    time.sleep(2)
    logger.debug("Creating new job...")
    Job.drop_collection()
    Worker.drop_collection()

    payload = dict(payload)
    payload.update(
        {
            "message_broker": config.TRANSPORT,
        }
    )

    job = Job(
        type="test",
        payload=payload,
        runnable={
            "image": "akadaner/test-worker",
            "python_main": python_main,
        },
        running_timeout=60,
    )
    job.save()


def test_job_orchestrator(config, payload=None):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(host=config.MONGODB_HOST, db=config.MONGODB_DB)
    logger.info("Connected to mongodb")

    controller = ProcessController()

    run_listener_async("job_orchestrator", message_broker=config.TRANSPORT)
    job_orchestrator = JobOrchestrator(controller, config.TRANSPORT)
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
