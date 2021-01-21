from utils_ak.log import configure_logging
from utils_ak.microservices import ProductionMicroservice, run_listener_async

configure_logging(stream=True)


class Heartbeater(ProductionMicroservice):
    pass


if __name__ == '__main__':
    heartbeater = Heartbeater(default_broker='zmq')
    run_listener_async('heartbeat', default_broker='zmq', brokers_config=heartbeater.brokers_config)
    heartbeater.run()
