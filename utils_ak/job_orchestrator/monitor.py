from datetime import datetime
from utils_ak.simple_microservice import SimpleMicroservice


class Monitor:
    def __init__(self, message_broker, heartbeat_timeout=10):
        self.microservice = SimpleMicroservice("Monitor", message_broker=message_broker)

        self.workers = {}  # {id: {status, state, last_heartbeat}}
        self.heartbeat_timeout = heartbeat_timeout

        self.microservice.add_timer(self._update_stalled, 3.0)
        self.microservice.add_callback("monitor_in", "heartbeat", self._on_heartbeat)
        self.microservice.add_callback("monitor_in", "state", self._on_state)
        self.microservice.register_publishers(["monitor_out"])

        # # todo: del, hardcode
        # self.received_messages_cache = []

    def _update_stalled(self):
        for worker_id, worker in self.workers.items():
            if "status" not in worker:
                # non-initialized
                continue

            if worker["status"] == "success":
                continue

            if (
                datetime.utcnow() - worker["last_heartbeat"]
            ).total_seconds() > self.heartbeat_timeout:
                self._update_status(worker_id, "stalled")

    def _on_heartbeat(self, topic, id):
        if id in self.workers:
            self.workers[id]["last_heartbeat"] = datetime.utcnow()

    def _update_status(self, worker_id, status):
        if status != self.workers[worker_id].get("status"):
            self.microservice.logger.info("Status updated", id=worker_id, status=status)
            self.microservice.publish(
                "monitor_out",
                "status_change",
                id=worker_id,
                old_status=self.workers[worker_id].get("status"),
                new_status=status,
                state=self.workers[worker_id].get("state", {}),
            )
            self.workers[worker_id]["status"] = status

    def _on_state(self, topic, id, status, state):
        # # todo: del, preview hardcode
        # if id is not None and state is not None:
        #     key = id + state
        #     if key not in self.received_messages_cache:
        #         self.received_messages_cache.append(key)
        #     else:
        #         return

        # init if necessary
        if id not in self.workers:
            self.microservice.publish("monitor_out", "new", id=id)
            self.workers[id] = {"last_heartbeat": datetime.utcnow()}

        self._update_status(id, status)

        # set state
        _old_state = self.workers[id].get("state")
        self.workers[id]["state"] = state
        if _old_state != state:
            self.microservice.logger.debug("New state", id=id, state=state)
