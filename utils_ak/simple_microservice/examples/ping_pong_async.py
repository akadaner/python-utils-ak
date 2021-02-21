import asyncio
import numpy as np
from datetime import datetime

from utils_ak.zmq import endpoint
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.loguru import configure_loguru_stdout


class Ping(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Test publisher", *args, **kwargs)
        self.add_callback("pong", "", self.send_ping)
        self.add_timer(self.print_random, 2.0)

    async def print_random(self):
        timeout = np.random.uniform(5.0, 6.0)
        self.logger.info(f"Stop: {datetime.now()} {timeout}")
        await asyncio.sleep(timeout)
        self.logger.info(f"Resume: {datetime.now()} {timeout}")

    async def send_ping(self, topic, msg):
        self.logger.info(f"Received", topic=topic, msg=msg)
        await asyncio.sleep(1.0)
        self.publish_json("ping", "", {"msg": "ping"})


class Pong(SimpleMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__("Test publisher", *args, **kwargs)
        self.add_callback("ping", "", self.send_pong)

    async def send_pong(self, topic, msg):
        self.logger.info(f"Received", topic=topic, msg=msg)
        await asyncio.sleep(1.0)
        self.publish_json("pong", "", {"msg": "pong"})


def run_ping():
    configure_loguru_stdout()
    ping = Ping(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "ping": {"type": "sub", "endpoint": endpoint("localhost", 6554)},
                    "pong": {"type": "sub", "endpoint": endpoint("localhost", 6555)},
                }
            },
        )
    )

    async def send_initial():
        await asyncio.sleep(1.0)
        await ping.send_ping("init", "init")

    ping.tasks.append(asyncio.ensure_future(send_initial()))
    ping.run()


def run_pong():
    configure_loguru_stdout()
    Pong(
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "ping": {"type": "sub", "endpoint": endpoint("localhost", 6554)},
                    "pong": {"type": "sub", "endpoint": endpoint("localhost", 6555)},
                }
            },
        )
    ).run()


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.Process(target=run_ping).start()
    multiprocessing.Process(target=run_pong).start()
