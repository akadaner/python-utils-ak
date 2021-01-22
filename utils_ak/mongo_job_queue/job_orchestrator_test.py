import multiprocessing
import time

from utils_ak.mongo_job_queue.job_orchestrator import JobOrchestrator
from utils_ak.mongo_job_queue.controller.controllers.local import LocalWorkerController
from utils_ak.mongo_job_queue.worker.factory.test import TestWorkerFactory
from utils_ak.mongo_job_queue.models import *

BROKER = 'zmq'
BROKER_CONFIG = {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}, 'monitor_out': {'endpoint': 'tcp://localhost:5556', 'type': 'sub'}}}
MESSAGE_BROKER = (BROKER, BROKER_CONFIG)

from mongoengine import connect


def create_new_job():
    connect()
    time.sleep(5)
    print('Creating new job...')
    Job.drop_collection()
    job = Job(type='test', payload={'type': 'batch'})
    job.save()


def test():
    connect()
    worker_factory = TestWorkerFactory(MESSAGE_BROKER)
    controller = LocalWorkerController(worker_factory)
    orchestrator = JobOrchestrator(controller, MESSAGE_BROKER)
    multiprocessing.Process(target=create_new_job).start()
    orchestrator.run()


if __name__ == '__main__':
    test()
