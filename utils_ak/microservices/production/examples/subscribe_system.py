import time
import asyncio

from utils_ak.microservices import ProductionMicroservice, run_listener_async
from utils_ak.log import configure_logging

BROKER = 'zmq'
configure_logging(stream=True)


class Publisher(ProductionMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_callback('system', '', callback=self.sure)

    def sure(self, topic, msg):
        self.logger.info('Publisher: ARE YOU SURE YOU WANNA DO THIS?')


class Stopper(ProductionMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_callback('system', '', callback=self.sure)

    def stop(self):
        self.logger.info('Stopping publisher')
        self.publish_json('system', '', {'instance_id': 'Publisher', 'type': 'stop'})
        time.sleep(1)
        self.logger.info('Stopping others')
        self.publish_json('system', '', {'type': 'stop'})

    def sure(self, topic, msg):
        self.logger.info('Stopper: ARE YOU SURE YOU WANNA DO THIS?')


def run_pub():
    Publisher('Publisher', default_broker=BROKER).run()


def run_stopper():
    time.sleep(1)
    stopper = Stopper('Stopper', default_broker=BROKER)

    async def stop():
        time.sleep(1)
        stopper.stop()
    stopper.tasks.append(asyncio.ensure_future(stop()))
    stopper.run()


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.Process(target=run_pub).start()
    multiprocessing.Process(target=run_stopper).start()
