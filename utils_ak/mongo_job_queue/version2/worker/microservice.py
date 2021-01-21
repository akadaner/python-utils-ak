from utils_ak.interactive_imports import *


class WorkerMicroservice(BaseMicroservice):
    def __init__(self, id, *args, **kwargs):
        super().__init__(f'Test Worker Microservice {id}', *args, **kwargs)
        self.add_timer(self.publish_json, 3., args=('executor_monitor', 'heartbeat', {'id': id},))


def test():
    ms = WorkerMicroservice('Worker')
    import multiprocessing
    multiprocessing.Process(target=ms.run).start()