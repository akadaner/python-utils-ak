import time
import asyncio
from loguru import logger
from utils_ak.simple_microservice import run_listener_async
from utils_ak.mongo_job_queue.worker.worker import Worker


class TestWorker(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
    from utils_ak.loguru import configure_loguru_stdout
    configure_loguru_stdout('INFO')
    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    time.sleep(2)
    worker = TestWorker('worker_id', {'type': 'batch'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker.run()
    logger.info('Finished batch')


def test_streaming():
    from utils_ak.loguru import configure_loguru_stdout
    configure_loguru_stdout('INFO')
    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker = TestWorker('worker_id', {'type': 'streaming'}, message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker.run()


def test_deployment():
    from utils_ak.loguru import configure_loguru_stdout
    configure_loguru_stdout('DEBUG')
    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))

    from utils_ak.deployment import DockerController
    ctrl = DockerController()

    import anyconfig
    deployment = anyconfig.load('deployment.yml')
    ctrl.stop(deployment['id'])
    ctrl.start(deployment)
    time.sleep(5)
    ctrl.stop(deployment['id'])


if __name__ == '__main__':
    test_batch()
    # test_streaming()
    # test_deployment()