from utils_ak.job_orchestrator.worker.test.test_worker import *
from utils_ak.job_orchestrator.config import settings
from utils_ak.deployment import *

MESSAGE_BROKER = settings.as_dict()["TRANSPORT"]["message_broker"]


def test_batch():
    test_microservice_worker(
        TestWorker,
        {"type": "batch", "message_broker": MESSAGE_BROKER},
        run_listener=True,
    )


def test_streaming():
    test_microservice_worker(
        TestWorker,
        {"type": "streaming", "message_broker": MESSAGE_BROKER},
        run_listener=True,
    )


def test_deployment():
    controller = ProcessController()
    test_microservice_worker_deployment(
        "../worker/test/sample_deployment.yml", controller, MESSAGE_BROKER
    )


if __name__ == "__main__":
    test_batch()
    # test_streaming()
    # test_deployment()
