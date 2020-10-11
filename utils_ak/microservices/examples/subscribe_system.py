import time
import logging
import asyncio
import random

from utils_ak.microservices import Microservice

from utils_ak.log import configure_stream_logging
from utils_ak.zmq import endpoint

configure_stream_logging(level=logging.INFO)

PUBLISHER_ID = random.randint(0, 10 ** 6)
BROKER = 'zmq'
BROKERS_CONFIG = {'zmq': {'endpoints': {'system': {'type': 'pub', 'endpoint': endpoint('localhost', 6554)}}}}

class Publisher(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__(f'Publisher - {PUBLISHER_ID}', system_enabled=True, *args, **kwargs)
        self.add_callback('system', 'stop', callback=self.sure)

    def sure(self, topic, msg):
        self.logger.info('Publisher: ARE YOU SURE YOU WANNA DO THIS?')


class Stopper(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Stopper', system_enabled=True, *args, **kwargs)
        self.add_callback('system', 'stop', callback=self.sure)

    def stop(self):
        self.logger.info('Stopping publisher')
        self.publish_json('system', '', {'instance_id': f'Publisher - {PUBLISHER_ID}', 'type': 'stop'})
        time.sleep(1)
        self.logger.info('Stopping others')
        self.publish_json('system', '', {'type': 'stop'})

    def sure(self, topic, msg):
        self.logger.info('Stopper: ARE YOU SURE YOU WANNA DO THIS?')


def run_pub():
    Publisher(default_broker=BROKER, brokers_config=BROKERS_CONFIG).run()


def run_stopper():
    stopper = Stopper(default_broker=BROKER, brokers_config=BROKERS_CONFIG)

    async def delayed_stop():
        await asyncio.sleep(2.0)
        stopper.stop()

    stopper.tasks.append(asyncio.ensure_future(delayed_stop()))
    stopper.run()


if __name__ == '__main__':
    import multiprocessing

    multiprocessing.Process(target=run_pub).start()
    multiprocessing.Process(target=run_stopper).start()
