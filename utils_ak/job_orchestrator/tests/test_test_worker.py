from utils_ak.job_orchestrator.worker.test.test_worker import *
from utils_ak.deployment import *


def test_batch():
    _test_batch_microservice_worker(
        TestWorker,
        {"type": "batch", "message_broker": MESSAGE_BROKER},
        run_listener=True,
    )


def test_streaming():
    _test_batch_microservice_worker(
        TestWorker,
        {"type": "streaming", "message_broker": MESSAGE_BROKER},
        run_listener=True,
    )


def test_deployment():
    controller = ProcessController()
    _test_microservice_worker_deployment(
        {"type": "batch", "message_broker": MESSAGE_BROKER},
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/python-utils-ak/utils_ak/job_orchestrator/worker/test/main.py",
        controller,
    )


if __name__ == "__main__":
    # test_batch()
    # test_streaming()
    test_deployment()
