from .broker import Broker
from .kafka_broker import KafkaBroker
from .zmq_broker import ZMQBroker


def cast_message_broker(obj):
    if isinstance(obj, Broker):
        return obj
    elif isinstance(obj, (list, tuple)):
        broker_type, broker_config = obj
        return {'zmq': ZMQBroker, 'kafka': KafkaBroker}[broker_type](**broker_config)