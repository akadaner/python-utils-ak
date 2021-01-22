import time
import logging
import multiprocessing


from utils_ak.log import configure_logging
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.mongo_job_queue.worker.workers.test import TestWorker
from utils_ak.mongo_job_queue.worker.monitor import MonitorActor

BROKER = 'zmq'
BROKER_CONFIG = {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}, 'monitor_out': {'endpoint': 'tcp://localhost:5556', 'type': 'sub'}}}
MESSAGE_BROKER = (BROKER, BROKER_CONFIG)


def run_monitor():
    configure_logging(stream_level=logging.DEBUG)
    ms = SimpleMicroservice('Monitor', message_broker=MESSAGE_BROKER)
    actor = MonitorActor(ms)
    ms.run()


def run_worker():
    configure_logging(stream_level=logging.DEBUG)
    worker = TestWorker('WorkerId', {'type': 'batch'}, message_broker=MESSAGE_BROKER)
    worker.run()


def test():
    configure_logging(stream_level=logging.DEBUG)
    run_listener_async('monitor_out', message_broker=MESSAGE_BROKER)
    time.sleep(1)

    multiprocessing.Process(target=run_monitor).start()
    time.sleep(3)
    multiprocessing.Process(target=run_worker).start()


if __name__ == '__main__':
    test()

