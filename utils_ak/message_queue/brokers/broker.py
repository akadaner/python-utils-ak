class Broker:
    async_supported = False

    def subscribe(self, collection, topic, *args, **kwargs):
        raise NotImplementedError

    def publish(self, collection, topic, msg):
        raise NotImplementedError

    def poll(self, timeout=0.0):
        raise NotImplementedError

    async def aiopoll(self, timeout=0.0):
        raise NotImplementedError
