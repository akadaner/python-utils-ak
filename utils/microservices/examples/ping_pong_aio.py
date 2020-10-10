import asyncio
from datetime import datetime
import numpy as np
import logging

from utils.zmq import endpoint
from utils.microservices import Microservice

logging.basicConfig(level=logging.INFO)

ping_logger = logging.getLogger('ping')
pong_logger = logging.getLogger('pong')

BROKER = 'zmq'
BROKERS_CONFIG = {'zmq': {'endpoints': {'ping': {'type': 'sub', 'endpoint': endpoint('localhost', 6554)},
                                        'pong': {'type': 'sub', 'endpoint': endpoint('localhost', 6555)}}}}

class Ping(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Test publisher', logger=ping_logger, *args, **kwargs)
        self.add_callback('pong', '', self.send_ping)
        self.add_timer(self.print_random, 2.0)

    async def print_random(self):
        timeout = np.random.uniform(5.0, 6.0)
        self.logger.info(f'Stop: {datetime.now()} {timeout}')
        await asyncio.sleep(timeout)
        self.logger.info(f'Resume: {datetime.now()} {timeout}')

    async def send_ping(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        await asyncio.sleep(1.0)
        self.publish_json('ping', '', {'msg': 'ping'})


class Pong(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Test publisher', logger=pong_logger, *args, **kwargs)
        self.add_callback('ping', '', self.send_pong)

    async def send_pong(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        await asyncio.sleep(1.0)
        self.publish_json('pong', '', {'msg': 'pong'})


def run_ping():
    ping = Ping(default_broker=BROKER, brokers_config=BROKERS_CONFIG)

    async def send_initial():
        await asyncio.sleep(1.0)
        await ping.send_ping('init', 'init')

    ping.tasks.append(asyncio.ensure_future(send_initial()))
    ping.run()


def run_pong():
    Pong(default_broker=BROKER, brokers_config=BROKERS_CONFIG).run()


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.Process(target=run_ping).start()
    multiprocessing.Process(target=run_pong).start()
