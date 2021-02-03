import time
import logging
import multiprocessing
from utils_ak.mongo_job_queue.controller.controller import WorkerController
from utils_ak.mongo_job_queue.worker.factory import TestWorkerFactory
from utils_ak.simple_microservice import run_listener_async


# processes work only when controller works - so we don't need to keep cache
class LocalWorkerController(WorkerController):
    def __init__(self, worker_factory):
        self.workers = {}  # {id: process}
        self.worker_factory = worker_factory

    def start_worker(self, id, type, payload):
        p = multiprocessing.Process(target=self._start_worker, args=(id, type, payload), daemon=True)
        self.workers[id] = p
        p.start()

    def _start_worker(self, id, type, payload):
        worker = self.worker_factory.make_worker(id, type, payload)
        worker.run()

    def stop_worker(self, id):
        # todo: send stop signal instead
        self.workers[id].kill()


def test():
    import logging
    from utils_ak import configure_logging
    configure_logging(stream_level=logging.DEBUG)
    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))

    worker_factory = TestWorkerFactory(message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    worker_manager = LocalWorkerController(worker_factory)
    worker_manager.start_worker('Worker 1', 'test', {'type': 'batch'})


if __name__ == '__main__':
    test()


