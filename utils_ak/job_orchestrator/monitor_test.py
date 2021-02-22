import time
import logging
import multiprocessing


from utils_ak.loguru import configure_loguru_stdout
from utils_ak.simple_microservice import SimpleMicroservice, run_listener_async
from utils_ak.job_orchestrator.worker.test import TestWorker
from utils_ak.job_orchestrator.monitor import Monitor

BROKER = "zmq"
BROKER_CONFIG = {
    "endpoints": {
        "monitor_in": {"endpoint": "tcp://localhost:5555", "type": "sub"},
        "monitor_out": {"endpoint": "tcp://localhost:5556", "type": "sub"},
    }
}
MESSAGE_BROKER = (BROKER, BROKER_CONFIG)


def run_monitor():
    configure_loguru_stdout("TRACE")
    monitor = Monitor(MESSAGE_BROKER)
    monitor.microservice.run()


def run_worker():
    configure_loguru_stdout("TRACE")
    worker = TestWorker("WorkerId", {"type": "batch", "message_broker": MESSAGE_BROKER})
    worker.run()


def test():
    configure_loguru_stdout("TRACE")
    run_listener_async("monitor_out", message_broker=MESSAGE_BROKER)
    time.sleep(1)

    multiprocessing.Process(target=run_monitor).start()
    time.sleep(3)
    multiprocessing.Process(target=run_worker).start()


if __name__ == "__main__":
    test()
