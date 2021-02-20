import logging

from utils_ak.zmq import endpoint
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.log import configure_stream_logging

configure_stream_logging(stream_level=logging.INFO)


class Publisher(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Publisher", *args, **kwargs)
        self.add_timer(self.timer_function, 2)

    def timer_function(self):
        self.publish_json("collection", "", {})


if __name__ == "__main__":
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
