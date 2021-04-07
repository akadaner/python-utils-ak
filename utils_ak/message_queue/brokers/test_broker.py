import time


def test_broker(broker, collection="collection", topic=""):
    message = "message".encode("utf-8")
    for i in range(100):
        broker.publish(collection, topic, message)

    time.sleep(1)
    broker.subscribe(collection, topic)

    i = 0
    while True:
        messages = broker.poll()

        if not messages:
            continue

        print(i, messages)

        i += 1
        if i == 100:
            break


def test_kafka_broker():
    from utils_ak.message_queue.brokers import KafkaBroker

    test_broker(
        KafkaBroker(consumer_config={"auto_offset_reset": "smallest"}),
        topic="",
    )


if __name__ == "__main__":
    test_kafka_broker()
