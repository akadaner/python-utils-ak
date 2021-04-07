from .broker import Broker
from .brokers import *


def cast_message_broker(obj):
    if isinstance(obj, Broker):
        return obj
    elif isinstance(obj, (list, tuple)):
        broker_type, broker_config = obj
        return {"zmq": ZMQBroker, "kafka": KafkaBroker}[broker_type](**broker_config)
    else:
        raise Exception("Unknown broker type")
