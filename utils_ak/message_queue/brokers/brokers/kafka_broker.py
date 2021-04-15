from utils_ak.message_queue.brokers.broker import Broker

from utils_ak.kafka import *
from loguru import logger


class KafkaBroker(Broker):
    def __init__(self, *args, **kwargs):
        self.cli = KafkaClient(*args, **kwargs)
        self.async_supported = False  # todo: switch to True?

    def _get_kafka_topic(self, collection, topic):
        if topic:
            return f"{collection}__{topic}"
        else:
            return collection

    def _split_kafka_topic(self, kafka_topic):
        if "__" in kafka_topic:
            return kafka_topic.split("__", 1)
        else:
            return kafka_topic, ""

    def publish(self, collection, topic, msg):
        self.cli.publish(self._get_kafka_topic(collection, topic), msg)

    def subscribe(self, collection, topic, start_offset=None):
        self.cli.subscribe(
            self._get_kafka_topic(collection, topic), start_offset
        )

    def poll(self, timeout=0.0):
        response = self.cli.poll(timeout)
        if not response:
            return
        records = sum(response.values(), [])
        # todo: optimize
        return [
            list(self._split_kafka_topic(record.topic)) + [record.value]
            for record in records
        ]
