import multiprocessing
from utils_ak.simple_microservice.microservice import SimpleMicroservice
from utils_ak.loguru import configure_loguru_stdout


class Listener(SimpleMicroservice):
    def __init__(self, collection, topic="", *args, **kwargs):
        super().__init__(f"Listener_{collection}", *args, **kwargs)
        self.add_callback(collection, topic, self._log, formatter="default")

    def _log(self, topic, **kwargs):
        self.logger.info(f"{topic}-{str(kwargs)}")


def run_listener(collection, topic="", *args, **kwargs):
    configure_loguru_stdout()
    Listener(collection, topic, *args, **kwargs).run()


def run_listener_async(collection, topic="", *args, **kwargs):
    args = [collection, topic] + list(args)
    multiprocessing.Process(target=run_listener, args=args, kwargs=kwargs).start()
