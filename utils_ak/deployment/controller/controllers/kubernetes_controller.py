import anyconfig
import copy
import os
import tempfile

from loguru import logger

from utils_ak.deployment.config import BASE_DIR
from utils_ak.deployment.controller import Controller
from utils_ak.serialization import cast_js, cast_dict_or_list
from utils_ak.os import *
from utils_ak.dict import fill_template


class KubernetesController(Controller):
    def start(self, deployment):
        id = deployment['id']
        assert len(deployment['containers']) == 1, "Only one-container pods are supported for now"

        # create docker-compose file and run it without building
        entity, container = list(deployment['containers'].items())[0]
        params = {'entity': entity, 'deployment_id': id, 'image': container['image']}

        configs = cast_dict_or_list(os.path.join(BASE_DIR, 'example/kubernetes.deployment.yml.template'))
        configs = [fill_template(config, **params) for config in configs]

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

    def stop(self, deployment_id):
        fn = f'data/kubernetes/{deployment_id}/kubernetes.yml'
        execute(f'kubectl delete -f "{fn}"')
        remove_path(f'data/kubernetes/{deployment_id}/')

    def log(self, deployment_id):
        logger.debug('Logs', logs=execute(f'kubectl logs -l deployment_id={deployment_id}'))


def test_kubernetes_controller():
    import anyconfig
    import time
    from utils_ak.loguru import logger, configure_loguru_stdout
    configure_loguru_stdout('DEBUG')

    deployment = anyconfig.load('../../example/deployment.yml')
    ctrl = KubernetesController()
    ctrl.start(deployment)
    time.sleep(10)
    ctrl.log(deployment['id'])
    ctrl.stop(deployment['id'])


if __name__ == '__main__':
    test_kubernetes_controller()