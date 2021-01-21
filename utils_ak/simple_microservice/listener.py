from utils_ak.simple_microservice.microservice import SimpleMicroservice
import multiprocessing


class Listener(SimpleMicroservice):
    def __init__(self, collection, topic='', *args, **kwargs):
        super().__init__(f'Listener_{collection}', *args, **kwargs)
        self.add_callback(collection, topic, self._log, formatter='default')

    def _log(self, topic, msg):
        self.logger.info(f'{topic}-{msg}')


def run_listener(collection, topic='', *args, **kwargs):
    Listener(collection, topic, *args, **kwargs).run()


def run_listener_async(collection, topic='', *args, **kwargs):
    args = [collection, topic] + list(args)
    multiprocessing.Process(target=run_listener, args=args, kwargs=kwargs).start()
