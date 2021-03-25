import asyncio
from datetime import datetime

from utils_ak.job_orchestrator.worker.worker import MicroserviceWorker
from utils_ak.job_orchestrator.worker.test_worker import *


class TestWorker(MicroserviceWorker):
    async def process(self):
        self.started = datetime.now()

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
        elif self.payload.get("type") == "streaming":
            time.sleep(3)
            time.sleep(self.payload.get("initializing_timeout", 0))
            self.send_state("running", {})
            time.sleep(self.payload.get("running_timeout", 0))
            while True:
                self.send_state("running", {"foo": "bar"})
                time.sleep(3)
                if (datetime.now() - self.started).total_seconds() > 10:
                    # work for one-minute only and than die
                    break
        else:
            raise Exception(f'Bad payload type {self.payload.get("type")}')

        self.microservice.stop()
