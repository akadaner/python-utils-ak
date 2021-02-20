import logging

from utils_ak.granular_storage.storage.fs_storage import MsgPackGranularStorage
from utils_ak.log import configure_stream_logging

configure_stream_logging(level=logging.INFO)

BROKER = "kafka"


class KafkaTopicLiveDumper(object):
    """
    Live kafka topic backup-er, stores all received messages to a given storage
    """

    def __init__(self, topic, table, timestamp_resolver):
        self.topic = topic
        self.table = table
        self.timestamp_resolver = timestamp_resolver

    def on_msg(self, topic, msg):
        """ Message there is considered to be a dict """

        if self.table is None:
            print(self.topic, msg)
        else:
            timestamp = self.timestamp_resolver(msg)
            self.table.store(timestamp, "", msg)


if __name__ == "__main__":
    topic = "binance_trades"

    dumper = KafkaTopicLiveDumper(
        topic=topic,
        table=MsgPackGranularStorage("/tmp/storage")["binance"].get_table(
            "binance_trades"
        ),
        timestamp_resolver=lambda msg: msg["T"],
    )

    msg = {"value": "hi", "T": 228}

    dumper.on_msg(topic=topic, msg=msg)
