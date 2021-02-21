from datetime import datetime


class MonitorActor:
    def __init__(self, microservice, heartbeat_timeout=10):
        self.microservice = microservice
        self.workers = {}  # {id: {status, state, last_heartbeat}}
        self.heartbeat_timeout = heartbeat_timeout

        self.microservice.add_timer(self._update_stalled, 3.0)
        self.microservice.add_callback("monitor_in", "heartbeat", self._on_heartbeat)
        self.microservice.add_callback("monitor_in", "state", self._on_state)

        # todo: del, hardcode
        self.received_messages_cache = []

    def _update_stalled(self):
        for worker_id in self.workers:
            if "status" not in self.workers[worker_id]:
                # non-initialized
                # todo: make properly
                continue

            if self.workers[worker_id]["status"] == "success":
                continue

            if (
                "last_heartbeat" in self.workers[worker_id]
                and (
                    datetime.utcnow() - self.workers[worker_id]["last_heartbeat"]
                ).total_seconds()
                > self.heartbeat_timeout
            ):
                self._update_status(worker_id, "stalled")

    def _on_heartbeat(self, topic, id):
        # todo: make properly
        if id in self.workers:
            self.workers[id]["last_heartbeat"] = datetime.utcnow()

    def _update_status(self, worker_id, status):
        if status != self.workers[worker_id].get("status"):
            self.microservice.publish(
                "monitor_out",
                "status_change",
                {
                    "id": worker_id,
                    "old_status": self.workers[worker_id].get("status"),
                    "new_status": status,
                },
            )
            self.workers[worker_id]["status"] = status

    def _on_state(self, topic, id, status, state):
        # todo: del, preview hardcode
        if id is not None and state is not None:
            key = id + state
            if key not in self.received_messages_cache:
                self.received_messages_cache.append(key)
            else:
                return

        if id not in self.workers:
            self.microservice.publish("monitor_out", "new", {"id": id})
            self.workers[id] = {}
            self._update_status(id, status)
            self.workers[id]["state"] = state

        self.microservice.logger.info("Current monitor state", state=self.workers)
