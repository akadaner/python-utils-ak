import logging
import random

from utils_ak.microservices import SystemMicroservice, run_listener_async
from utils_ak.log import configure_stream_logging


configure_stream_logging(stream_level=logging.INFO)

BROKER = 'zmq'
BROKERS_CONFIG = {'zmq': {'endpoints': {'heartbeat': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'} }}}


class Heartbeater(SystemMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(f'Publisher {random.randint(0, 10 ** 6)}', heartbeat_freq=1, *args, **kwargs)


if __name__ == '__main__':
    run_listener_async('heartbeat', brokers_config=BROKERS_CONFIG, default_broker=BROKER)
    Heartbeater(brokers_config=BROKERS_CONFIG, default_broker=BROKER).run()
