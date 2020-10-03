from confluent_kafka import Producer, Consumer, TopicPartition
import uuid


# https://github.com/edenhill/librdkafka/wiki/Manually-setting-the-consumer-start-offset

class KafkaAdapter:
    """ Allows to read. """

    def __init__(self, bootstrap_servers='localhost:9092', auto_offset_reset='largest'):
        self.kafka_topics = []
        self.state = None
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': str(uuid.uuid4()),
            'default.topic.config': {
                'auto.offset.reset': auto_offset_reset
            },
            'enable.auto.commit': False,
            'enable.partition.eof': False,
        })

        self.topic = None
        self.topic_partition = None

    def subscribe(self, topic, offset=0, partition=0):
        self.topic = topic
        self.topic_partition = TopicPartition(topic, partition, offset)
        self.consumer.assign([self.topic_partition])
        # self.seek()

    def seek(self):
        self.consumer.seek(self.topic_partition)

    def receive(self, number):
        return self.consumer.consume(number, timeout=1)

    def has_messages(self):
        smallest, largest = self.get_edge_offsets()
        # TODO: check for by-one blunders
        return self.consumer.position([self.topic_partition])[0].offset <= largest

    def get_edge_offsets(self):
        smallest, largest = self._get_watermarks()
        return smallest, largest

    # Get the interval in which offsets reside
    def _get_watermarks(self):
        return self.consumer.get_watermark_offsets(self.topic_partition)


if __name__ == "__main__":
    adapter = KafkaAdapter()
    adapter.subscribe("benchmark1", 99999)

    i = 0
    while True:
        msg = adapter.receive(1)
        if not msg:
            continue
        msg = msg[0]
        print(msg.offset())
        i += 1
        if i == 10:
            break

    # message = a.consumer.consume(1)[0]
    # print(message.error(), message.value(), message.topic())
