from utils_ak.message_queue.brokers.rabbitmq_broker import RabbitMQBroker


class Microservice(object):
    def __init__(self, callback=None, logger=None, serializer=None):
        self.broker = RabbitMQBroker(callback=callback)
        self.is_active = True

    def stop(self):
        self.is_active = False

    def publish(self, queue, msg):
        self.broker.publish(queue, msg)

    def subscribe(self, queue):
        self.broker.subscribe(queue)

    def start_consuming(self, queue):
        self.broker.start_consuming(queue)

    def add_callback(self, callback=None):
        self.broker.callback = callback