import time
import logging
import zmq

from utils_ak.str import b, u
from utils_ak.coder import JsonCoder

CONTEXT = zmq.Context()

logger = logging.getLogger(__name__)


def endpoint(host, port):
    return f"tcp://{host}:{port}"


def split_endpoint(endpoint):
    protocol, host, port = endpoint.split(":")
    return protocol, host[2:], port


def cast_connection(address, conn_type):
    protocol, host, port = split_endpoint(address)
    if conn_type == "auto":
        conn_type = "bind" if "*" in address else "connect"
    elif conn_type == "bind":
        address = f"tcp://*:{port}"
    elif conn_type == "connect":
        if host == "*":
            raise Exception(f"Cannot connect to specified address: {address}")
    return address, conn_type


def connect_socket(socket, address, conn_type):
    address, conn_type = cast_connection(address, conn_type)
    # "tcp://*:5563"
    if conn_type == "bind":
        socket.bind(address)
    elif conn_type == "connect":
        socket.connect(address)
    else:
        raise Exception(f"Connection type {conn_type} not recognized")


class Publisher(object):
    def __init__(
        self,
        address,
        sndhwm=0,
        init_timeout=0.1,
        context=CONTEXT,
        conn_type="auto",
        json_coder=JsonCoder(),
    ):
        self.context = context
        self.socket = context.socket(zmq.PUB)
        self.address = address
        self.json_coder = json_coder

        if sndhwm is not None:
            # self.socket.SNDHWM = sndhwm
            self.socket.setsockopt(zmq.SNDHWM, sndhwm)
        connect_socket(self.socket, address, conn_type)
        time.sleep(init_timeout)

    def term(self):
        self.socket.close()

    def publish(self, topic, msg):
        # todo: fix msg stuff!!!
        self.socket.send_multipart([b(topic), b(msg)])

    def publish_json(self, topic, msg):
        self.socket.send_multipart([b(topic), b(self.json_coder.encode(msg))])


class Subscriber(object):
    def __init__(self, address, rcvhwm=None, context=CONTEXT, conn_type="auto"):
        # Prepare our context and publisher
        self.context = context
        self.socket = context.socket(zmq.SUB)
        self.address = address

        if rcvhwm is not None:
            self.socket.setsockopt(zmq.RCVHWM, rcvhwm)

        connect_socket(self.socket, address, conn_type)

        self.topics = []

    def subscribe(self, topic, timeout=0.01):
        topic = b(topic)

        if topic in self.topics:
            logger.warning(f"Already subscribed for topic {u(topic)}")
            return

        self.socket.setsockopt(zmq.SUBSCRIBE, topic)
        time.sleep(timeout)

    def receive(self):
        multipart_msg = self.socket.recv_multipart()
        topic, contents = multipart_msg[-2:]
        return topic, contents

    def term(self):
        self.socket.close()


class Forwarder(object):
    """ For multi pub-sub communication. """

    def __init__(self, endpoint_in, endpoint_out, context=CONTEXT):
        self.endpoint_in = endpoint_in
        self.endpoint_out = endpoint_out
        self.context = context

    def run(self):
        try:
            # Socket facing clients
            logging.info(
                f"Forwarder. Frontend (IN): {self.endpoint_in} Backend (OUT): {self.endpoint_out}"
            )
            self.frontend_socket = self.context.socket(zmq.SUB)
            connect_socket(self.frontend_socket, self.endpoint_in, "bind")
            self.frontend_socket.setsockopt(zmq.SUBSCRIBE, b(""))
            self.backend_socket = self.context.socket(zmq.PUB)
            connect_socket(self.backend_socket, self.endpoint_out, "bind")
            zmq.device(zmq.FORWARDER, self.frontend_socket, self.backend_socket)
        except Exception as e:
            logger.error("Exception occured. Bringing down forwarder device")
        finally:
            self.frontend_socket.close()
            self.backend_socket.close()


class Streamer(object):
    """ For multi push-pull communication. """

    def __init__(self, endpoint_in, endpoint_out, context=CONTEXT):
        self.endpoint_in = endpoint_in
        self.endpoint_out = endpoint_out
        self.context = context

    def run(self):
        try:
            # Socket facing clients
            logging.info(
                f"Streamer. Frontend (IN): {self.endpoint_in} Backend (OUT): {self.endpoint_out}"
            )

            self.frontend_socket = self.context.socket(zmq.PULL)
            connect_socket(self.frontend_socket, self.endpoint_in, "bind")

            self.backend_socket = self.context.socket(zmq.PUSH)
            connect_socket(self.backend_socket, self.endpoint_out, "bind")
            zmq.device(zmq.STREAMER, self.frontend_socket, self.backend_socket)
        except Exception as e:
            logger.error("Exception occured. Bringing down streamer device")
        finally:
            self.frontend_socket.close()
            self.backend_socket.close()
