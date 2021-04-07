from confluent_kafka import Producer, Consumer
import uuid

from utils_ak.builtin import update_dic
from copy import deepcopy

from utils_ak.kafka.kafka_client import KafkaClient


def test():
    cli = KafkaClient(
        consumer_config={"default.topic.config": {"auto.offset.reset": "smallest"}}
    )
    cli.subscribe("collection__topic")

    i = 0
    while True:
        msg = cli.poll()
        if not msg:
            continue
        print(msg, msg.offset(), msg.value())
        print(i)

        i += 1


if __name__ == "__main__":
    test()
