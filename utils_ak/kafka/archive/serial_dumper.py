from utils_ak.kafka.archive.kafka_adapter import KafkaAdapter

import logging

logger = logging.getLogger(__name__)


class KafkaTopicSerialDumper:
    def __init__(
        self,
        topic,
        kafka_adapter,
        table,
        state_provider,
        parser,
        batch=1000,
        serializer=None,
        one_store_limit=None,
    ):
        self.topic = topic
        self.kafka_adapter = kafka_adapter
        self.table = table
        self.state_provider = state_provider
        self.parser = parser
        self.batch = batch
        self.serializer = serializer
        self.state = None
        self.one_store_limit = one_store_limit

    def __str__(self):
        return f"KafkaTopicSerialDumper with topic {self.topic}"

    def store(self):
        self.state = self.state_provider.get_state()
        offset = self.state["offset"]
        self.kafka_adapter.subscribe(self.topic, offset, 0)

        smallest_offset, largest_offset = self.kafka_adapter.get_edge_offsets()
        logger.debug(
            f"smallest_offset={smallest_offset}, largest_offset={largest_offset}"
        )

        if smallest_offset > offset:
            logger.warning(
                f"Missed messages with offsets: {offset} to {smallest_offset}"
            )
            offset = smallest_offset
            self.kafka_adapter.subscribe(self.topic, offset, 0)

        logger.debug(f"Subscribed from offset={offset}")

        total = 0
        last_offset = offset
        finished = False

        sp = self.state_provider

        while not finished:
            kafka_msgs = self.kafka_adapter.receive(self.batch)

            if len(kafka_msgs) == 0:
                finished = True
            else:
                current_offset = None
                for kafka_msg in kafka_msgs:
                    if kafka_msg.offset() > largest_offset or (
                        self.one_store_limit is not None
                        and total >= self.one_store_limit
                    ):
                        finished = True
                        break

                    total += 1

                    if kafka_msg.error() is not None:
                        logging.error(kafka_msg.error())  # TODO: log

                    msg = kafka_msg.value()
                    if self.serializer:
                        msg = self.serializer.decode(msg)

                    for timestamp, key, msg in self.parser(msg, kafka_msg):
                        self.table.store(timestamp, key, msg)

                    current_offset = kafka_msg.offset()
                if current_offset is not None:
                    last_offset = current_offset
                    sp.set_state({"offset": current_offset + 1})

        self.table.flush()
        lag = largest_offset - last_offset
        logger.info(
            f"current_offset={last_offset}, "
            f"largest_offset={largest_offset}, "
            f"lag={lag}"
        )
        return total, lag


if __name__ == "__main__":
    from utils_ak.log import configure_stream_logging

    configure_stream_logging(level=logging.DEBUG)

    from utils_ak.granular_storage.storage import JsonGranularStorage
    from utils_ak.state.provider import PickleDBStateProvider
    from utils_ak.serialization.serializer import JsonSerializer

    sd = KafkaTopicSerialDumper(
        topic="binance_trades",
        kafka_adapter=KafkaAdapter(),
        table=JsonGranularStorage("/tmp/storage")
        .get_db("binance")
        .get_table("binance/user_feed", pattern="%Y%m%d_{base}"),
        state_provider=PickleDBStateProvider(
            "/tmp/binance_trades_state.json", default_state={"offset": 0}
        ),
        parser=lambda value, msg: (value["T"], None, value),
        serializer=JsonSerializer(),
    )
    sd.store()
