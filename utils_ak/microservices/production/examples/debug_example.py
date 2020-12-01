import time
from utils_ak.log import configure_logging

from utils_ak.microservices import ProductionMicroservice

configure_logging(stream=True)


class DebugMicroservice(ProductionMicroservice):
    def __init__(self):
        super().__init__()

        self.task_started = False
        self.add_schedule(self.task, "0 * * * * *")

    def task(self):
        self.logger.info("Task started")
        if not self.task_started:
            time.sleep(2 * 60)
            self.task_started = True

        self.logger.info("Task stopped")


if __name__ == '__main__':
    DebugMicroservice().run()
