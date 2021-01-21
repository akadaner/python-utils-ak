import zmq

from utils_ak.zmq.wrappers import Subscriber, Publisher, endpoint


class ZMQClient:
    def __init__(self, context=None):
        super().__init__()
        self.poller = None
        self.context = context or zmq.Context()

        self.subscribers = {}
        self.publishers = {}

    def subscribe(self, endpoint, topic=None, conn_type='auto'):
        if not self.poller:
            self.poller = zmq.Poller()
        topic = topic or ''
        if endpoint not in self.subscribers:
            sub = Subscriber(endpoint, context=self.context, conn_type=conn_type)
            self.subscribers[endpoint] = sub
            self._register(sub)
        self.subscribers[endpoint].subscribe(topic)

    def _register(self, receiver):
        self.poller.register(receiver.socket, zmq.POLLIN)

    def _create_publisher(self, endpoint, conn_type='auto'):
        if endpoint not in self.publishers:
            self.publishers[endpoint] = Publisher(endpoint, context=self.context, conn_type=conn_type)

    def publish(self, endpoint, topic, msg, conn_type='auto'):
        self._create_publisher(endpoint, conn_type=conn_type)
        self.publishers[endpoint].publish(topic, msg)

    def poll(self, timeout=0.):
        if not self.subscribers:
            return

        if len(self.subscribers) == 1 and timeout == 0:
            endpoint, sub = list(self.subscribers.items())[0]
            topic, msg = sub.receive()
            return endpoint, topic, msg
        else:
            socks = dict(self.poller.poll(timeout * 1000))
            for endpoint, receiver in self.subscribers.items():
                if receiver.socket in socks and socks[receiver.socket] == zmq.POLLIN:
                    topic, msg = receiver.receive()
                    return endpoint, topic, msg
