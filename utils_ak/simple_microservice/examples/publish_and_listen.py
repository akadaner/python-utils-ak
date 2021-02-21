import logging

from utils_ak.zmq import endpoint
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.loguru import configure_loguru_stdout


class Publisher(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Publisher", *args, **kwargs)
        self.add_timer(self.timer_function, 2)

    def timer_function(self):
        self.publish_json("collection", "", {})


if __name__ == "__main__":
    configure_loguru_stdout()
    run_listener_async(
        "collection",
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "collection": {
                        "type": "sub",
                        "endpoint": endpoint("localhost", 6554),
                    }
                }
            },
        ),
    )
    Publisher(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "collection": {
                        "type": "sub",
                        "endpoint": endpoint("localhost", 6554),
                    }
                }
            },
        )
    ).run()
