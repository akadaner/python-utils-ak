import asyncio
import time
import logging

from utils_ak.log import configure_logging
from utils_ak.simple_microservice import run_listener_async
from utils_ak.mongo_job_queue.worker.worker import Worker
from utils_ak.mongo_job_queue.worker.microservice import WorkerMicroservice



class TestWorker(Worker):
    def __init__(self, id, payload, message_broker):
        super().__init__(id, payload)
        self.microservice = WorkerMicroservice(f'Test Worker Microservice {id}', message_broker=message_broker)

    async def process(self, payload):
        if payload['type'] == 'batch':
            await asyncio.sleep(3)
            self.microservice.send_state({'status': 'running'})
            for i in range(5):
                self.microservice.send_state({'progress': (i + 1) * 20})
                await asyncio.sleep(1)
            self.microservice.send_state({'status': 'success'})
            self.microservice.stop()

        elif payload['type'] == 'streaming':
            await asyncio.sleep(3)
            self.microservice.send_state({'status': 'running'})

            while True:
                self.microservice.send_state({'foo': 'bar'})
                await asyncio.sleep(3)

    def run(self):
        async def send_initial():
            await asyncio.sleep(0.1)
            await self.process(self.payload)

        self.microservice.tasks.append(asyncio.ensure_future(send_initial()))
        self.microservice.run()


def test_batch():
    configure_logging(stream_level=logging.INFO)

    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    time.sleep(1)
    worker = TestWorker('WorkerId', {'type': 'batch'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker.run()


def test_streaming():
    configure_logging(stream_level=logging.INFO)

    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    time.sleep(1)
    worker = TestWorker('WorkerId', {'type': 'streaming'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker.run()


if __name__ == '__main__':
    # test_batch()
    test_streaming()