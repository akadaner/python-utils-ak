import time
import asyncio

from utils_ak.zmq import endpoint
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.loguru import configure_loguru_stdout


class Ping(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Ping", *args, **kwargs)
        self.add_callback("ping", "", self.send_ping)
        self.add_timer(self.send_ping, interval=1, n_times=1, args=("init", "init"))

    def send_ping(self, topic, msg):
        self.logger.info(f"Received", topic=topic, msg=msg)
        time.sleep(1)
        self.publish("pong", "", msg="ping")


class Pong(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Pong", *args, **kwargs)
        self.add_callback("pong", "", self.send_pong)

    def send_pong(self, topic, msg):
        self.logger.info(f"Received", topic=topic, msg=msg)
        time.sleep(1)
        self.publish("ping", "", msg="pong")


def run_ping():
    configure_loguru_stdout("DEBUG")
    ping = Ping(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "ping": {"type": "sub", "endpoint": endpoint("localhost", 6554)},
                    "pong": {"type": "sub", "endpoint": endpoint("localhost", 6555)},
                }
            },
        ),
    )

    ping.run()


def run_pong():
    configure_loguru_stdout("DEBUG")

    Pong(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "ping": {"type": "sub", "endpoint": endpoint("localhost", 6554)},
                    "pong": {"type": "sub", "endpoint": endpoint("localhost", 6555)},
                }
            },
        ),
    ).run()


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.Process(target=run_ping).start()
    multiprocessing.Process(target=run_pong).start()
