import multiprocessing

from utils_ak.simple_microservice import *


class WorkerMicroservice(SimpleMicroservice):
    def __init__(self, id, *args, **kwargs):
        super().__init__(id, *args, **kwargs)
        self.add_timer(
            self.publish,
            3.0,
            args=(
                "monitor",
                "heartbeat",
            ),
            kwargs={"id": self.id},
        )

    def send_state(self, status, state):
        self.publish("monitor", "state", id=self.id, status=status, state=state)
