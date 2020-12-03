import anyconfig

from utils_ak.microservices import SystemMicroservice
from utils_ak.serialization import MsgPackSerializer
from utils_ak.config.config import get_config


class ProductionMicroservice(SystemMicroservice):
    def __init__(self, microservice_id=None, config=None, logger=None, heartbeat_freq=3, extra_config=None, default_broker='zmq', *args, **kwargs):
        self.config = config or get_config(require_local=True)
        if extra_config:
            anyconfig.merge(self.config, extra_config, ac_merge=anyconfig.MS_DICTS)

        # # todo: make properly
        # if 'microservice_name' not in self.config:
        #     self.config['microservice_name'] = 'default'
        # if 'configuration' not in self.config:
        #     self.config['configuration'] = 'default'

        microservice_id = microservice_id or '-'.join([self.config['microservice_name'], self.config['configuration']])

        self.brokers_config = {'zmq': {'endpoints': self.config['zmq']['endpoints']}}

        super().__init__(
            microservice_id,
            microservice_name=self.config.get('microservice_name'),
            logger=logger,
            heartbeat_freq=heartbeat_freq,
            system_enabled=True,
            serializer=MsgPackSerializer(),
            brokers_config=self.brokers_config,
            default_broker=default_broker or self.config['broker'],
            asyncio_support=self.config.get('asyncio_support', True))
