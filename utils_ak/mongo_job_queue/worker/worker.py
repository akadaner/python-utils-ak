import time
import os
import asyncio
from loguru import logger
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.coder import cast_dict_or_list


class Worker:
    def __init__(self, id, payload):
        self.id = id
        self.payload = payload


class MicroserviceWorker(Worker):
    def __init__(self, id, payload):
        super().__init__(id, payload)
        self.microservice = SimpleMicroservice(
            id, message_broker=self.payload["message_broker"]
        )

        self.microservice.add_timer(
            self.microservice.publish,
            3.0,
            args=(
                "monitor",
                "heartbeat",
            ),
            kwargs={"id": self.id},
        )

    def send_state(self, status, state):
        self.microservice.publish(
            "monitor", "state", id=self.id, status=status, state=state
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
