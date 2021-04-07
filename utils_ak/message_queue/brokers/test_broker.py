def test_broker(broker):
    collection = "collection"
    topic = "topic"
    # message = "message".encode("utf-8")
    # for i in range(100):
    #     broker.publish(collection, topic, message)

    broker.subscribe(collection, topic)

    i = 0
    while True:
        msg = broker.poll()

        if not msg:
            continue

        print(i, msg)

        i += 1
        if i == 100:
            break


def test_kafka_broker():
    from utils_ak.message_queue.brokers import KafkaBroker

    test_broker(
        KafkaBroker(
            consumer_config={"default.topic.config": {"auto.offset.reset": "smallest"}}
        )
    )


if __name__ == "__main__":
    test_kafka_broker()
