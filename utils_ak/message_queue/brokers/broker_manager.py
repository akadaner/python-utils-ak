""" Broker manager is a builtin of multiple brokers. """
from .kafka_broker import KafkaBroker
from .zmq_broker import ZMQBroker

BrokerTypes = {'zmq': ZMQBroker, 'kafka': KafkaBroker}


def get_broker_class(broker_name):
    if broker_name in BrokerTypes:
        return BrokerTypes[broker_name]
    else:
        raise Exception(f'Unknown broker_type {broker_name}')


# todo: optimize broker initialization


class BrokerManager:
    def __init__(self, default_broker='zmq', brokers_config=None):
        # {broker: broker_config}
        self.brokers_config = brokers_config or {}
        self.default_broker_name = default_broker.lower()

        self.brokers = {}

        # {broker: {builtin: [topic1, topic2, ...]}}
        self.subscribed_to = {}

    def get_broker(self, broker_name=None):
        broker_name = self._get_broker_name(broker_name)
        if broker_name not in self.brokers:
            self.brokers[broker_name] = BrokerTypes[broker_name](**self.brokers_config.get(broker_name, {}))
        return self.brokers[broker_name]

    def _get_broker_name(self, broker_name=None):
        return broker_name or self.default_broker_name

    def subscribe(self, collection, topic, broker_name=None):
        broker_name = self._get_broker_name(broker_name)

        if not self._is_subscribed(broker_name, collection, topic):
            self.get_broker(broker_name).subscribe(collection, topic)
            self.subscribed_to.setdefault(broker_name, {}).setdefault(collection, []).append(topic)

    def publish(self, collection, topic, msg, broker_name=None):
        self.get_broker(broker_name).publish(collection, topic, msg)

    def poll(self, timeout=0., broker_name=None):
        broker_name = self._get_broker_name(broker_name)
        if broker_name not in self.subscribed_to:
            return
        return self.get_broker(broker_name).poll(timeout)

    def _is_subscribed(self, broker_name, collection, topic):
        return topic in self.subscribed_to.get(broker_name, {}).get(collection, [])

    def support_async(self, broker_name):
        return self.get_broker(broker_name).support_async

    def has_subscriptions(self, broker_name):
        return broker_name in self.subscribed_to
