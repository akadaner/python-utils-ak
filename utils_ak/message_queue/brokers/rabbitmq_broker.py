from .broker import Broker
from utils_ak.message_queue.clients.rabbitmq_client import RabbitMQClient


class RabbitMQBroker(Broker):
    def __init__(self, callback=None):
        self.cli = RabbitMQClient(callback=callback)
        self.async_supported = False

    def _add_callback(self, callback):
        # todo: refactor
        self.cli.callback = callback

    def publish(self, queue, msg):
        self.cli.publish(queue, msg)

    def subscribe(self, queue):
        self.cli.subscribe(queue)

    def start_consuming(self, queue, collection=None):
        self.cli.start_consuming(queue)

    def poll(self, timeout=0.):
        pass
