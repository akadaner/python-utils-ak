import time
import asyncio

from utils_ak.microservices import ProductionMicroservice, run_listener_async
from utils_ak.log import configure_logging

BROKER = 'zmq'
configure_logging(stream=True)


class Ping(ProductionMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Ping microservice', *args, **kwargs)
        self.add_callback('ping', '', self.send_ping)
        self.add_timer(self.timer_function, 2)
        self.add_schedule(self.schedule_function, freq='3s')

    def send_ping(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        time.sleep(1)
        self.publish_json('pong', '', {'msg': 'ping'})

    def timer_function(self):
        self.logger.info('This is a timer function')

    def schedule_function(self):
        self.logger.info('This is a schedule function')


class Pong(ProductionMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Pong microservice', *args, **kwargs)
        self.add_callback('pong', '', self.send_pong)

    def send_pong(self, topic, msg):
        self.logger.info(f'Received {topic} {msg}')
        time.sleep(1)
        self.publish_json('ping', '', {'msg': 'pong'})


def run_ping():
    ping = Ping(default_broker=BROKER)

    async def send_initial():
        await asyncio.sleep(1.0)
        ping.send_ping('init', 'init')

    ping.tasks.append(asyncio.ensure_future(send_initial()))
    ping.run()


def run_pong():
    Pong(default_broker=BROKER).run()


if __name__ == '__main__':
    import multiprocessing

    multiprocessing.Process(target=run_ping).start()
    multiprocessing.Process(target=run_pong).start()
