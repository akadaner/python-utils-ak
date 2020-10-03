from utils_ak.time.dt import cast_sec

from .base import BaseMicroservice


class Microservice(BaseMicroservice):
    def __init__(self, microservice_id, logger=None, heartbeat_freq=None, system_enabled=False,
                 microservice_name=None, serializer=None, default_broker='zmq', brokers_config=None,
                 asyncio_support=True):
        super().__init__(logger, serializer=serializer, default_broker=default_broker,
                         brokers_config=brokers_config, asyncio_support=asyncio_support)

        self.microservice_name = microservice_name or microservice_id
        self.microservice_id = microservice_id

        # Heartbeats
        self.heartbeat_freq = heartbeat_freq
        if heartbeat_freq:
            self.heartbeat_sec = cast_sec(self.heartbeat_freq)
            self.add_timer(self.publish_json, heartbeat_freq,
                           args=('heartbeat', '', {'name': self.microservice_name},))

        # Subscribe to system commands
        if system_enabled:
            self.add_callback('system', '', callback=self.on_stop,
                              filter=[self.check_id, lambda topic, msg: msg['type'] == 'stop'])

    def check_id(self, topic, msg):
        if 'instance_id' not in msg:
            return True
        else:
            instance_ids = msg['instance_id']
            if not isinstance(instance_ids, list):
                instance_ids = [instance_ids]
            return self.microservice_id in instance_ids

    def on_stop(self, topic, msg):
        self.logger.info(f'Service stopping {self.microservice_id}')
        self.is_active = False
