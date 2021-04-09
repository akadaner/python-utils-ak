import itertools
from utils_ak.message_queue.brokers.broker import Broker


class MultiBroker(Broker):
    def __init__(self, brokers, subscribe_broker, publish_broker):
        self.brokers = brokers
        self.subscribe_broker = subscribe_broker
        self.publish_broker = publish_broker

        self.brokers_iterator = itertools.cycle(self.brokers)

    def publish(self, collection, topic, msg):
        self.publish_broker.publish(collection, topic, msg)

    def subscribe(self, collection, topic):
        self.subscribe_broker.subscribe(collection, topic)

    def poll(self, timeout=0.0):
        cur_broker = next(self.brokers_iterator)
        return cur_broker.poll(timeout)
