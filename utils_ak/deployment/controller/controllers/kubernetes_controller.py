import anyconfig
import copy
import os
import tempfile
import yaml

from io import StringIO
from loguru import logger
from utils_ak.deployment.controller import Controller
from utils_ak.serialization import cast_js
from utils_ak.os import *


class KubernetesController(Controller):
    def start(self, deployment):
        id = deployment['id']
        assert len(deployment['containers']) == 1, "Only one-container pods are supported for now"

        # create docker-compose file and run it without building
        with open('../../example/kubernetes.deployment.yml.template', 'r') as f:
            template_str = f.read()
        entity, container = list(deployment['containers'].items())[0]
        params = {'entity': entity, 'deployment_id': id, 'image': container['image']}
        config = template_str.format(**params)

        configs = list(yaml.load_all(StringIO(config)))

        for config in configs:
            if config['kind'] == 'ConfigMap':
                for k, v in container['command_line_arguments'].items():
                    config['data'][k.upper()] = cast_js(v)
        config_str = '---\n'.join([anyconfig.dumps(config, 'yaml') for config in configs])

        makedirs(f'data/kubernetes/{id}/')
        fn = f'data/kubernetes/{id}/kubernetes.yml'

        with open(fn, 'w') as f:
            f.write(config_str)

        execute(f'kubectl apply -f "{fn}"')

    def stop(self, deployment):
        id = deployment['id']
        fn = f'data/kubernetes/{id}/kubernetes.yml'
        execute(f'kubectl delete -f "{fn}"')
        remove_path(f'data/kubernetes/{id}/')

    def log(self, deployment):
        logger.debug('Logs', logs=execute(f'kubectl logs -l deployment_id={deployment["id"]}'))


def test_kubernetes_controller():
    import anyconfig
    import time
    from utils_ak.loguru import logger, configure_loguru_stdout
    configure_loguru_stdout('DEBUG')

    deployment = anyconfig.load('../../example/deployment.yml')
    ctrl = KubernetesController()
    ctrl.start(deployment)
    time.sleep(10)
    ctrl.log(deployment)
    ctrl.stop(deployment)


if __name__ == '__main__':
    test_kubernetes_controller()