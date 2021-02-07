import asyncio
from utils_ak.mongo_job_queue.worker.worker import Worker
from utils_ak.mongo_job_queue.worker.worker_test import *


class TestWorker(Worker):
    async def process(self):
        if self.payload.get('type') == 'batch':
            await asyncio.sleep(1)
            self.microservice.send_state('running', {})
            for i in range(5):
                self.microservice.send_state('running', {'progress': (i + 1) * 20})
                await asyncio.sleep(0.1)
            self.microservice.send_state('success', {'response': '42'})
            self.microservice.stop()
        elif self.payload.get('type') == 'streaming':
            await asyncio.sleep(3)
            self.microservice.send_state('running', {})

            while True:
                self.microservice.send_state('running', {'foo': 'bar'})
                await asyncio.sleep(3)
        else:
            raise Exception(f'Bad payload type {self.payload.get("type")}')


def test_batch():
    test_worker(TestWorker, {'type': 'batch'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))


def test_streaming():
    test_worker(TestWorker, {'type': 'streaming'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))


def test_deployment():
    test_worker_deployment('deployment.yml', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))


if __name__ == '__main__':
    # test_batch()
    # test_streaming()
    test_deployment()