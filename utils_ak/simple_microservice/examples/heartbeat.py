import logging
import random
import time

from utils_ak.log import configure_stream_logging
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
configure_stream_logging(stream_level=logging.INFO)


class Heartbeater(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(f'Publisher {random.randint(0, 10 ** 6)}', *args, **kwargs)
        self.add_timer(self.publish_json, 1.0, args=('monitor', 'asdf', {'id': self.id},))


if __name__ == '__main__':
    BROKER = 'zmq'
    BROKERS_CONFIG = {'zmq': {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}}
    run_listener_async('monitor', brokers_config=BROKERS_CONFIG, default_broker=BROKER)
    Heartbeater(brokers_config=BROKERS_CONFIG, default_broker=BROKER).run()