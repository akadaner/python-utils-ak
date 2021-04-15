import bisect
import functools
from utils_ak.dict import *
from utils_ak.loguru import *
from loguru import logger

# NOTE: WORKING WITH SINGLE-PARTITIONED KAFKA TOPICS


def get_single_topic_partition(kafka_consumer, topic):
    if len(kafka_consumer.assignment()) == 0:
        # init kafka and reset offsets
        record = next(kafka_consumer)
        partitions = list(kafka_consumer.assignment())

        record_partition = [
            p
            for p in partitions
            if p.topic == record.topic and p.partition == record.partition
        ][0]
        kafka_consumer.seek(record_partition, max(record.offset - 1, 0))

    partitions = list(kafka_consumer.assignment())

    topic_partitions = [p for p in partitions if p.topic == topic]
    assert len(topic_partitions) == 1
    return topic_partitions[0]


def get_record_by_offset(kafka_consumer, topic, offset):
    # fetch first to init
    if offset < 0:
        offset = offset % get_end_offset(kafka_consumer, topic)

    partition = get_single_topic_partition(kafka_consumer, topic)
    kafka_consumer.seek(partition, offset)
    return next(kafka_consumer)


def get_end_offset(kafka_consumer, topic):
    partition = get_single_topic_partition(kafka_consumer, topic)
    return kafka_consumer.end_offsets(kafka_consumer.assignment())[partition]


@functools.total_ordering
class _KafkaRecord:
    def __init__(self, record, lt_key):
        self.record = record
        self.lt_key = lt_key

    def __lt__(self, other):
        return self.lt_key(self.record) < self.lt_key(other.record)


class KafkaConsumerAsList:
    def __init__(self, kafka_consumer, topic, lt_key):
        self.kafka_consumer = kafka_consumer
        self.topic = topic
        self._length = None
        self.lt_key = lt_key

    def __len__(self):
        if not self._length:
            self._length = get_end_offset(self.kafka_consumer, self.topic)
        return self._length

    def __getitem__(self, item):
        if not isinstance(item, int):
            raise KeyError
        item = item % len(self)
        record = get_record_by_offset(self.kafka_consumer, self.topic, item)
        return self.cast_list_record(record)

    def cast_list_record(self, record):
        if isinstance(record, dict):
            record = dotdict(record)
        return _KafkaRecord(record, self.lt_key)


TOPIC = "datasets__60705b5edaf0f2d1693a39c6"


def test():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    consumer.assignment()
    print(get_record_by_offset(consumer, TOPIC, 0))
    print(get_record_by_offset(consumer, TOPIC, 1))
    print(get_record_by_offset(consumer, TOPIC, 0))
    print(get_record_by_offset(consumer, TOPIC, -1))


def test_kafka_bisect_left():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    consumer_as_list = KafkaConsumerAsList(
        consumer, TOPIC, lt_key=lambda record: record.timestamp
    )

    print(
        bisect.bisect_left(
            consumer_as_list,
            consumer_as_list.cast_list_record({"timestamp": 1617976164019}),
        )
    )


if __name__ == "__main__":
    test()
    test_kafka_bisect_left()
