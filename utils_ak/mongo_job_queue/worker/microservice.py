import multiprocessing

from utils_ak.interactive_imports import *

configure_logging(stream_level=logging.INFO)


class WorkerMicroservice(SimpleMicroservice):
    def __init__(self, id, *args, **kwargs):
        super().__init__(f'Test Worker Microservice {id}', *args, **kwargs)
        self.add_timer(self.publish_json, 3.0, args=('monitor', 'heartbeat', {'id': self.id},))

    def send_state(self, state):
        self.publish_json('monitor', 'state', {'id': self.id, 'state': state})


def test():
    run_listener_async('monitor', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    ms = WorkerMicroservice('Worker', message_broker=('zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://localhost:5555', 'type': 'sub'}}}))
    ms.run()


if __name__ == '__main__':
    test()