import asyncio
from utils_ak.job_orchestrator.worker.worker import MicroserviceWorker
from utils_ak.job_orchestrator.worker.worker_test import *


class TestWorker(MicroserviceWorker):
    async def process(self):
        if self.payload.get("type") == "batch":
            await asyncio.sleep(1)
            self.send_state("running", {})
            for i in range(5):
                self.send_state("running", {"progress": (i + 1) * 20})
                await asyncio.sleep(0.1)
            self.send_state("success", {"response": "42"})
            self.microservice.stop()
        elif self.payload.get("type") == "streaming":
            await asyncio.sleep(3)
            self.send_state("running", {})

            while True:
                self.send_state("running", {"foo": "bar"})
                await asyncio.sleep(3)
        else:
            raise Exception(f'Bad payload type {self.payload.get("type")}')


def test_batch():
    test_microservice_worker(
        TestWorker,
        {
            "type": "batch",
            "message_broker": (
                "zmq",
                {
                    "endpoints": {
                        "monitor_in": {
                            "endpoint": "tcp://localhost:5555",
                            "type": "sub",
                        }
                    }
                },
            ),
        },
    )


def test_streaming():
    test_microservice_worker(
        TestWorker,
        {
            "type": "streaming",
            "message_broker": (
                "zmq",
                {
                    "endpoints": {
                        "monitor_in": {
                            "endpoint": "tcp://localhost:5555",
                            "type": "sub",
                        }
                    }
                },
            ),
        },
    )


def test_deployment():
    test_microservice_worker_deployment(
        "sample_deployment.yml",
        message_broker=(
            "zmq",
            {
                "endpoints": {
                    "monitor_in": {"endpoint": "tcp://localhost:5555", "type": "sub"}
                }
            },
        ),
    )


if __name__ == "__main__":
    # test_batch()
    # test_streaming()
    test_deployment()
