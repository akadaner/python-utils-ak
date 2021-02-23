import asyncio
from utils_ak.job_orchestrator.worker.worker import MicroserviceWorker
from utils_ak.job_orchestrator.worker.worker_test import *


class TestWorker(MicroserviceWorker):
    async def process(self):
        running_timeout = self.payload.get("running_timeout", 0)
        if self.payload.get("type") == "batch":
            time.sleep(1)
            time.sleep(self.payload.get("initializing_timeout", 0))
            self.send_state("running", {})
            time.sleep(self.payload.get("stalled_timeout", 0))
            N = 10
            for i in range(N):
                self.send_state("running", {"progress": (i + 1) * 100 / N})
                await asyncio.sleep(0.1 + running_timeout / N)
            self.send_state("success", {"response": "42"})
            self.microservice.stop()
        elif self.payload.get("type") == "streaming":
            time.sleep(3)
            time.sleep(self.payload.get("initializing_timeout", 0))
            self.send_state("running", {})
            time.sleep(self.payload.get("running_timeout", 0))
            while True:
                self.send_state("running", {"foo": "bar"})
                time.sleep(3)
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
        run_listener=False,
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
        run_listener=False,
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
    test_batch()
    # test_streaming()
    # test_deployment()
