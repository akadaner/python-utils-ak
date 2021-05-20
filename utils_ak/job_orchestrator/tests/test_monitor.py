import time
import multiprocessing


from utils_ak.loguru import configure_loguru_stdout
from utils_ak.simple_microservice import run_listener_async
from utils_ak.job_orchestrator.worker.sample_worker.sample_worker import SampleWorker
from utils_ak.job_orchestrator.monitor import Monitor
from utils_ak.job_orchestrator.tests.config.config import config


def run_monitor():
    configure_loguru_stdout("DEBUG")
    monitor = Monitor(config.TRANSPORT)
    monitor.microservice.run()


def run_worker():
    configure_loguru_stdout("DEBUG")
    worker = SampleWorker(
        "WorkerId",
        {"type": "batch", "message_broker": config.TRANSPORT},
    )
    worker.run()


def run():
    configure_loguru_stdout("DEBUG")
    run_listener_async("monitor_out", message_broker=config.TRANSPORT)
    time.sleep(1)

    multiprocessing.Process(target=run_monitor).start()
    time.sleep(3)
    multiprocessing.Process(target=run_worker).start()


if __name__ == "__main__":
    run()
