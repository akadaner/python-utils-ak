import bisect
import functools
from utils_ak.dict import *


# NOTE: WORKING WITH SINGLE-PARTITIONED KAFKA TOPICS

# todo: implement kafka consumer as a list


def get_record_by_offset(kafka_consumer, offset):
    # fetch first to init
    if offset < 0:
        offset = offset % get_end_offset(kafka_consumer)

    next(kafka_consumer)
    partition = list(kafka_consumer.assignment())[0]
    kafka_consumer.seek(partition, offset)
    return next(kafka_consumer)


def get_end_offset(kafka_consumer):
    next(kafka_consumer)
    partition = list(kafka_consumer.assignment())[0]
    return kafka_consumer.end_offsets(kafka_consumer.assignment())[partition]


def kafka_bisect_left(
    kafka_consumer, timestamp, key=lambda record: record.timestamp, low=0, high=None
):

    if not high:
        high = get_end_offset(kafka_consumer)

    @functools.total_ordering
    class _KafkaRecord:
        def __init__(self, record):
            self.record = record

        def __lt__(self, other):
            return key(self.record) < key(other.record)

    class _KafkaGetter:
        def __getitem__(self, item):
            return _KafkaRecord(get_record_by_offset(kafka_consumer, item))

    _kafka_getter = _KafkaGetter()
    _value_record = _KafkaRecord(dotdict({"timestamp": timestamp}))
    offset = bisect.bisect_left(_kafka_getter, _value_record, low, high)
    print(offset)
    print(get_record_by_offset(kafka_consumer, offset))


def test():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        "datasets__60705b5edaf0f2d1693a39c6",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    consumer.assignment()
    print(get_record_by_offset(consumer, 0))
    print(get_record_by_offset(consumer, 1))
    print(get_record_by_offset(consumer, 0))
    print(get_record_by_offset(consumer, -1))


def test_kafka_bisect_left():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        "datasets__60705b5edaf0f2d1693a39c6",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print(
        kafka_bisect_left(consumer, 1617976164019, key=lambda record: record.timestamp)
    )


if __name__ == "__main__":
    test()
    test_kafka_bisect_left()
