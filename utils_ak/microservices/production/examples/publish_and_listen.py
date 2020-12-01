from utils_ak.microservices import ProductionMicroservice, run_listener_async
from utils_ak.log import configure_logging

BROKER = 'zmq'
configure_logging(stream=True)


class Publisher(ProductionMicroservice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_timer(self.timer_function, 2)

    def timer_function(self):
        self.publish('col', 'topic', 'msg')


if __name__ == '__main__':
    pub = Publisher(default_broker=BROKER)
    run_listener_async('col', 'topic', default_broker=BROKER, brokers_config=pub.brokers_config)
    pub.run()
