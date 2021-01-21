import multiprocessing


class WorkerManager:
    def start_worker(self, id, type, payload):
        raise NotImplemented

    def disable_worker(self, id):
        raise NotImplemented

