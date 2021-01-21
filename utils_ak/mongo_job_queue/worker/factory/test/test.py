import uuid

from utils_ak.mongo_job_queue.worker.factory.worker_factory import WorkerFactory
from utils_ak.mongo_job_queue.worker.workers import TestWorker


class TestWorkerFactory(WorkerFactory):
    def __init__(self, message_broker):
        self.message_broker = message_broker

    def make_worker(self, id, type, payload):
        assert type == 'test', 'Only test worker type is supported by test worker factory'
        return TestWorker(id, payload, self.message_broker)
