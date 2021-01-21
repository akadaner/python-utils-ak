import asyncio
import time

from utils_ak.interactive_imports import *

BROKER = 'zmq'
configure_logging(stream=True)



class TestWorker:
    def __init__(self, id, payload):
        self.id = id
        self.payload = payload
        self.microservice = WorkerMicroservice(f'Test Worker Microservice {id}', default_broker='zmq', brokers_config={''})

    async def process(self, payload):
        if payload['type'] == 'batch':
            await asyncio.sleep(5)
            self.microservice.

    def run(self):
        async def send_initial():
            await asyncio.sleep(1.0)
            await self.process(self.payload)

        ping.tasks.append(asyncio.ensure_future(send_initial()))
        self.microservice.run()