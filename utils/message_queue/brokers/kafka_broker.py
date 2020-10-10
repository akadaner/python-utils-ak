from .broker import Broker
from utils.message_queue.clients.kafka_client import KafkaClient

import logging

logger = logging.getLogger(__name__)


class KafkaBroker(Broker):
    def __init__(self):
        self.cli = KafkaClient()

    def _get_kafka_topic(self, collection, topic):
        if topic:
            return f'{collection}__{topic}'
        else:
            return collection

    def _split_kafka_topic(self, kafka_topic):
        if '__' in kafka_topic:
            return kafka_topic.split('__', 1)
        else:
            return kafka_topic, ''

    def publish(self, collection, topic, msg):
        self.cli.publish(self._get_kafka_topic(collection, topic), msg)

    def subscribe(self, collection, topic):
        self.cli.subscribe(self._get_kafka_topic(collection, topic))

    def poll(self, timeout=0.):
        received_message = self.cli.poll(timeout)

        if not received_message:
            return

        kafka_topic, msg = received_message.topic(), received_message.value()

        if not kafka_topic:
            logging.error(f'Empty kafka_topic received: kafka_topic={kafka_topic}, msg={msg}')
            raise Exception('Empty kafka_topic received')

        # todo: kafka_topic can be emtpy. Why?
        # print(received_message.offset())
        # msg["__kafka_offset"] = received_message.offset()

        collection, topic = self._split_kafka_topic(kafka_topic)
        return collection, topic, msg
