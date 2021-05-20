from utils_ak.simple_microservice import SimpleMicroservice

from .worker import Worker


class MicroserviceWorker(Worker):
    """ Working using SimpleMicroservice class from utils_ak.simple_microservice util. """
    def __init__(self, id, payload):
        super().__init__(id, payload)
        self.microservice = SimpleMicroservice(
            id, message_broker=self.payload["message_broker"]
        )

        self.microservice.add_timer(
            self.microservice.publish,
            interval=3.0,
            args=(
                "monitor_in",
                "heartbeat",
            ),
            kwargs={"id": self.id},
        )

        self.microservice.add_timer(
            self.on_start, n_times=1, counter_type="left"
        )  # run once on init

    def send_state(self, status, state):
        self.microservice.logger.debug("Sending state", status=status, state=state)
        self.microservice.publish(
            "monitor_in", "state", id=self.id, status=status, state=state
        )

    async def on_start(self):
        pass

    async def on_input(self, topic, msg):
        raise NotImplementedError

    def run(self):
        self.microservice.run()
