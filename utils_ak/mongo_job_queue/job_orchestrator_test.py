import multiprocessing
import time
import sys

from utils_ak.deployment import *
from utils_ak.loguru import configure_loguru_stdout
from utils_ak.mongo_job_queue.job_orchestrator import JobOrchestrator
from utils_ak.mongo_job_queue.models import *

from loguru import logger

BROKER = 'zmq'
BROKER_CONFIG = {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}, 'monitor_out': {'endpoint': 'tcp://localhost:5556', 'type': 'sub'}}}
MESSAGE_BROKER = (BROKER, BROKER_CONFIG)

from mongoengine import connect


def create_new_job():
    connect()
    logger.remove()
    configure_loguru_stdout()
    time.sleep(2)
    logger.debug('Creating new job...')
    Job.drop_collection()
    Worker.drop_collection()
    job = Job(type='test', payload={'type': 'batch'})
    job.save()


def test():
    connect()
    logger.remove()
    configure_loguru_stdout()
    controller = DockerController()
    orchestrator = JobOrchestrator(controller, MESSAGE_BROKER)
    multiprocessing.Process(target=create_new_job).start()
    orchestrator.run()


if __name__ == '__main__':
    test()
