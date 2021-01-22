import multiprocessing


class WorkerController:
    def start_worker(self, id, type, payload):
        raise NotImplemented

    def stop_worker(self, id):
        raise NotImplemented

