from .broker import Broker
from utils_ak.message_queue.clients.zmq_client import ZMQClient

ENDPOINT_TYPES = ["sub", "pub"]


class ZMQBroker(Broker):
    def __init__(self, endpoints):
        """
        :param endpoints: {collection_name: type, ...}
        """

        self.endpoints = endpoints

        # {endpoint: collection}
        self.collections = {}

        for collection, config in endpoints.items():
            if config["type"] in ["pub", "sub"]:
                self.collections[config["endpoint"]] = collection
            else:
                # forwarder
                self.collections[config["endpoint_pub"]] = collection
                self.collections[config["endpoint_sub"]] = collection

        self.cli = ZMQClient()

        self.async_supported = False

    def _get_endpoint_info(self, collection, action):
        """
        :param collection: str
        :param action: sub or pub
        :return:
        """
        config = self.endpoints[collection]
        endpoint_type = config["type"]
        if endpoint_type in ["pub", "sub"]:
            endpoint = config["endpoint"]
            conn_type = "bind" if endpoint_type == action else "connect"
        elif endpoint_type == "forwarder":
            if action == "pub":
                endpoint = config["endpoint_pub"]
            else:
                endpoint = config["endpoint_sub"]
            conn_type = "connect"
        else:
            raise Exception(f"Unsupported endpoint type {endpoint_type}")
        return endpoint, conn_type

    def subscribe(self, collection, topic):
        endpoint, conn_type = self._get_endpoint_info(collection, "sub")
        self.cli.subscribe(endpoint, topic, conn_type=conn_type)

    def publish(self, collection, topic, msg):
        endpoint, conn_type = self._get_endpoint_info(collection, "pub")
        self.cli.publish(endpoint, topic, msg, conn_type=conn_type)

    def poll(self, timeout=0.0):
        msg = self.cli.poll(timeout)
        if not msg:
            return
        endpoint, topic, msg = msg
        return self.collections[endpoint], topic, msg
