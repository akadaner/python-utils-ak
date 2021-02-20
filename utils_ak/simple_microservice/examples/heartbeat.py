import logging
import random
import time

from utils_ak.interactive_imports import *

configure_stream_logging(stream_level=logging.INFO)


class Heartbeater(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(f"Publisher {random.randint(0, 10 ** 6)}", *args, **kwargs)
        self.add_timer(
            self.publish_json,
            1.0,
            args=(
                "monitor",
                "asdf",
                {"id": self.id},
            ),
        )


if __name__ == "__main__":
    run_listener_async(
        "monitor",
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "monitor": {"endpoint": "tcp://localhost:5555", "type": "sub"}
                }
            },
        ),
    )
    Heartbeater(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "monitor": {"endpoint": "tcp://localhost:5555", "type": "sub"}
                }
            },
        )
    ).run()
