from utils_ak.job_orchestrator.worker.test.test_worker import *

from utils_ak.job_orchestrator.tests.config import settings

MESSAGE_BROKER = settings.as_dict()["TRANSPORT"]["message_broker"]


def test_batch():
    test_microservice_worker(
        TestWorker,
        {"type": "batch", "message_broker": MESSAGE_BROKER},
        run_listener=False,
    )


def test_streaming():
    test_microservice_worker(
        TestWorker,
        {"type": "streaming", "message_broker": MESSAGE_BROKER},
        run_listener=False,
    )


def test_deployment():
    # todo: test
    test_microservice_worker_deployment("sample_deployment.yml", MESSAGE_BROKER)


if __name__ == "__main__":
    # test_batch()
    test_streaming()
    # test_deployment()
