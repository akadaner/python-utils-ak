import time
import os
import asyncio
from loguru import logger
from utils_ak.mongo_job_queue.worker.microservice import WorkerMicroservice
from utils_ak.coder import cast_dict_or_list


class Worker:
    def __init__(self, id, payload):
        self.id = id
        self.payload = payload


class MicroserviceWorker(Worker):
    def __init__(self, id, payload):
        super().__init__(id, payload)
        self.microservice = WorkerMicroservice(
            id, message_broker=self.payload["message_broker"]
        )

    async def process(self):
        raise NotImplemented

    def run(self):
        async def send_initial():
            await asyncio.sleep(0.1)
            await self.process()

        self.microservice.tasks.append(asyncio.ensure_future(send_initial()))
        self.microservice.run()


def run_worker(worker_cls, config=None):
    config = config or os.environ.get("CONFIG")
    assert config is not None, "Config not specified"
    config = cast_dict_or_list(config)
    worker = worker_cls(config["worker_id"], config["payload"])
    worker.run()
