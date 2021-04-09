from confluent_kafka import Producer, Consumer
import uuid
from datetime import datetime, timedelta
from utils_ak.builtin import update_dic
from utils_ak.time import *
from copy import deepcopy
import time
from utils_ak.kafka.kafka_client import KafkaClient


def test():
    cli = KafkaClient(consumer_config={"auto_offset_reset": "smallest"})

    start_dt = datetime.now()
    for i in range(10):
        cli.publish("collection", str(i).encode("utf-8"))
        time.sleep(0.1)
    end_dt = datetime.now()

    cli.flush()
    time.sleep(1)
    cli.subscribe(
        "collection",
        # start_offset=689,
        start_dt=start_dt + (end_dt - start_dt) / 2,
    )

    i = 0
    while True:
        msg = cli.poll()
        if not msg:
            continue
        print(msg)
        print(i)
        i += 1


if __name__ == "__main__":
    test()
