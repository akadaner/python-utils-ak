import time
import multiprocessing
from loguru import logger
from mongoengine import connect as connect_to_mongodb

from utils_ak.simple_microservice import run_listener_async
from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout

from utils_ak.job_orchestrator.models import *
from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.tests.test_monitor import run_monitor
from utils_ak.job_orchestrator.tests.config.config import config
from utils_ak.job_orchestrator.worker.sample_worker.main import MAIN


def create_new_job(payload, python_main=MAIN, drop_collections=True):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(host=config.MONGODB_HOST, db=config.MONGODB_DB)
    logger.info("Connected to mongodb")
    time.sleep(2)

    if drop_collections:
        logger.info("Dropping Job and Worker collections...")
        Job.drop_collection()
        Worker.drop_collection()

    logger.debug("Creating new job...")
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


def run_job_orchestrator(payload=None):
    configure_loguru_stdout("DEBUG")
    connect_to_mongodb(host=config.MONGODB_HOST, db=config.MONGODB_DB)
    logger.info("Connected to mongodb")
    controller = ProcessController()

    run_listener_async("job_orchestrator", message_broker=config.TRANSPORT)
    logger.info('Running job orchestrator...')
    job_orchestrator = JobOrchestrator(controller, config.TRANSPORT)
    multiprocessing.Process(target=run_monitor).start()
    if payload:
        multiprocessing.Process(target=create_new_job, args=(payload,)).start()
    job_orchestrator.run()


def run_success():
    run_job_orchestrator({"type": "batch"})


def run_stalled():
    run_job_orchestrator({"type": "batch", "stalled_timeout": 600})


def run_timeout():
    run_job_orchestrator({"type": "batch", "running_timeout": 20})


def run_failure():
    run_job_orchestrator({"type": "batch", "initializing_timeout": 600})


def run():
    run_job_orchestrator()


if __name__ == "__main__":
    run_success()
    # run_stalled()
    # run_timeout()
    # run_failure()
    # run()
