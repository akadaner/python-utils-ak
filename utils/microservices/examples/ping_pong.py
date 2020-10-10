import time
import asyncio

from utils.zmq import endpoint
from utils.microservices import Microservice

import logging
logging.basicConfig(level=logging.INFO)

ping_logger = logging.getLogger('ping')
pong_logger = logging.getLogger('pong')

BROKER = 'zmq'
BROKERS_CONFIG = {'zmq': {'endpoints': {'ping': {'type': 'sub', 'endpoint': endpoint('localhost', 6554)},
                                        'pong': {'type': 'sub', 'endpoint': endpoint('localhost', 6555)}}}}


class Ping(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Test publisher', *args, **kwargs)
        self.add_callback('ping', '', self.send_ping)

    def send_ping(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        time.sleep(1)
        self.publish_json('pong', '', {'msg': 'ping'})


class Pong(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Test publisher', *args, **kwargs)
        self.add_callback('pong', '', self.send_pong)

    def send_pong(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        time.sleep(1)
        self.publish_json('ping', '', {'msg': 'pong'})


def run_ping():
    ping = Ping(logger=ping_logger, default_broker=BROKER, brokers_config=BROKERS_CONFIG)

    async def send_initial():
        await asyncio.sleep(1.0)
        ping.send_ping('init', 'init')

    ping.tasks.append(asyncio.ensure_future(send_initial()))
    ping.run()


def run_pong():
    Pong(logger=pong_logger, default_broker=BROKER, brokers_config=BROKERS_CONFIG).run()


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.Process(target=run_ping).start()
    multiprocessing.Process(target=run_pong).start()
