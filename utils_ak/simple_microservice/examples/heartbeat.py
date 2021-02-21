import random

from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.loguru import configure_loguru_stdout


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
    configure_loguru_stdout()
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
