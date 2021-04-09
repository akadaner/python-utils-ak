import kafka  # todo: move to kafka fully

import uuid

from utils_ak.builtin import update_dic
from copy import deepcopy

from utils_ak.kafka.kafka_binary_search import *


DEFAULT_CONSUMER_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "group_id": str(uuid.uuid4()),  # todo: make properly
    # "default.topic.config": {"auto.offset.reset": "largest"},
    # "enable.auto.commit": False,
    # "enable.partition.eof": False,
}

DEFAULT_PRODUCER_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    # "queue.buffering.max.ms": 1,
    # "queue.buffering.max.messages": 1000000,
    # "max.in.flight.requests.per.connection": 1,
    # "default.topic.config": {"acks": "all"},
}


class KafkaClient:
    def __init__(self, host=None, consumer_config=None, producer_config=None):
        self.start_offsets = {}

        consumer_config = consumer_config or {}
        self.consumer_config = deepcopy(DEFAULT_CONSUMER_CONFIG)
        self.consumer_config = update_dic(self.consumer_config, consumer_config)
        if host:
            self.consumer_config["bootstrap_servers"] = host
        self.consumer = kafka.KafkaConsumer(**self.consumer_config)

        producer_config = producer_config or {}
        self.producer_config = deepcopy(DEFAULT_PRODUCER_CONFIG)
        self.producer_config = update_dic(self.producer_config, producer_config)
        if host:
            self.producer_config["bootstrap_servers"] = host
        self.producer = kafka.KafkaProducer(**self.producer_config)

        self.init_subscriptions = False

    def subscribe(self, topic, start_offset=None, start_dt=None):
        if topic not in self.start_offsets:
            self.start_offsets[topic] = (start_offset, start_dt)

    def publish(self, topic, msg):
        self.producer.send(topic, msg)

    def flush(self, timeout=0):
        # self.producer.flush(timeout)
        pass

    def poll(self, timeout=0.0):
        self.start_listening()
        return self.consumer.poll(timeout)

    def start_listening(self):
        if not self.init_subscriptions and self.start_offsets:
            self.consumer.subscribe(list(self.start_offsets.keys()))
            for topic, (start_offset, start_dt) in self.start_offsets.items():
                if start_dt is not None:
                    start_offset = kafka_bisect_left(
                        self.consumer, topic, int(start_dt.timestamp() * 1000)
                    )
                if start_offset is not None:
                    partition = get_single_topic_partition(self.consumer, topic)
                    self.consumer.seek(partition, start_offset)
            self.init_subscriptions = True

    def _get_kafka_topics(self):
        consumer = kafka.KafkaConsumer(
            group_id=str(uuid.uuid4()), bootstrap_servers="localhost:9092"
        )
        return consumer.topics()
