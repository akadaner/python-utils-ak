class Worker:
    def __init__(self, id, payload):
        self.id = id
        self.payload = payload

    def run(self):
        raise NotImplementedError



def run_worker(worker_cls, worker_config):
    worker = worker_cls(worker_config["worker_id"], worker_config["payload"])
    worker.run()
