from loguru import logger

from kafka.admin import KafkaAdminClient as VanillaKafkaAdminClient
from kafka.admin import NewTopic
from utils_ak.kafka.kafka_client import KafkaClient

from utils_ak.tqdm import *


class KafkaAdminClient:
    def __init__(self, host):
        self.admin_client = VanillaKafkaAdminClient(bootstrap_servers=host)
        self.client = KafkaClient(host)

    def create_topics(self, topics, num_partitions=1, replication_factor=1):
        new_topics = [
            NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            for topic in topics
        ]
        return self.admin_client.create_topics(
            new_topics=new_topics, validate_only=False
        )

    def list_topics(self):
        return self.client.consumer.topics()

    def delete_topics(self, topics):
        self.admin_client.delete_topics(topics, timeout_ms=300)
        # todo: check for status


def test():
    from uuid import uuid4

    cli = KafkaAdminClient("localhost:9092")
    new_topic = str(uuid4())
    cli.create_topics([new_topic])
    time.sleep(5)
    assert new_topic in cli.list_topics()
    cli.delete_topics([new_topic])
    time.sleep(1)
    assert new_topic not in cli.list_topics()


def test_print_topics():
    cli = KafkaAdminClient("localhost:9092")
    print(cli.list_topics())


def test_delete_by_pattern(pattern):
    from fnmatch import fnmatch

    cli = KafkaAdminClient("localhost:9092")
    topics = cli.list_topics()
    topics = [topic for topic in topics if fnmatch(topic, pattern)]
    cli.delete_topics(topics)
    new_topics = cli.list_topics()
    assert all([topic not in topics for topic in new_topics])


if __name__ == "__main__":
    # test()
    test_print_topics()
    # test_delete_by_pattern("datasets__*")
