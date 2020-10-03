import logging

from utils_ak.zmq import endpoint
from utils_ak.microservices import Microservice, run_listener_async

from utils_ak.logging import configure_stream_logging
configure_stream_logging(level=logging.INFO)

BROKER = 'zmq'
COLLECTION = 'test_collection'
BROKERS_CONFIG = {'zmq': {'endpoints': {COLLECTION: {'type': 'sub', 'endpoint': endpoint('localhost', 6554)}}}}


class Publisher(Microservice):
    def __init__(self, *args, **kwargs):
        super().__init__('Publisher', *args, **kwargs)
        self.add_timer(self.timer_function, 2)

    def timer_function(self):
        self.publish(COLLECTION, '', 'bar_msg')


if __name__ == '__main__':
    run_listener_async(COLLECTION, default_broker=BROKER, brokers_config=BROKERS_CONFIG)
    Publisher(default_broker=BROKER, brokers_config=BROKERS_CONFIG).run()
