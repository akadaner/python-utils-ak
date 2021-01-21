class Broker:
    async_supported = False

    def subscribe(self, collection, topic):
        raise NotImplementedError

    def publish(self, collection, topic, msg):
        raise NotImplementedError

    def poll(self, timeout=0.):
        raise NotImplementedError

    async def aiopoll(self, timeout=0.):
        raise NotImplementedError

