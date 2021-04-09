def get_record_by_offset(kafka_consumer, offset):
    # fetch first to init
    next(kafka_consumer)
    partition = list(kafka_consumer.assignment())[0]
    kafka_consumer.seek(partition, offset)
    return next(kafka_consumer)


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


if __name__ == "__main__":
    test()
