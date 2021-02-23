import multiprocessing
import time
from utils_ak.simple_microservice.microservice import SimpleMicroservice
from utils_ak.loguru import configure_loguru_stdout


class Listener(SimpleMicroservice):
    def __init__(self, collection, topic="", *args, **kwargs):
        super().__init__(f"Listener_{collection}", *args, **kwargs)
        self.add_callback(collection, topic, self._log)

    def _log(self, topic, **kwargs):
        self.logger.info("Listened", topic=topic, **kwargs)


def run_listener(collection, topic="", *args, **kwargs):
    configure_loguru_stdout("DEBUG")
    Listener(collection, topic, *args, **kwargs).run()


def run_listener_async(collection, topic="", timeout=None, *args, **kwargs):
    args = [collection, topic] + list(args)
    if timeout:
        time.sleep(timeout)
    multiprocessing.Process(target=run_listener, args=args, kwargs=kwargs).start()
