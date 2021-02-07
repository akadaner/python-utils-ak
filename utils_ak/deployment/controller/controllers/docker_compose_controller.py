import anyconfig
import copy
import os
import tempfile
from utils_ak.deployment.controller import Controller
from utils_ak.serialization import cast_js
from utils_ak.os import *

# todo: success and failure responses (or errors?)

class DockerController(Controller):
    def start(self, deployment):
        id = deployment['id']

        assert len(deployment['containers']) == 1, "Only one-container pods are supported for now"

        # create docker-compose file and run it without building
        with open('../../example/docker-compose.template', 'r') as f:
            template_str = f.read()
        entity, container = list(deployment['containers'].items())[0]
        params = {'entity': entity, 'container_name': id + '_0', 'image': container['image']}
        config = template_str.format(**params)
        config = anyconfig.loads(config, 'yaml')

        for k, v in container['command_line_arguments'].items():
            config['services'][entity]['command'].append(f'--{k}')
            config['services'][entity]['command'].append(cast_js(v))

        makedirs(f'data/{id}/')
        fn = f'data/{id}/docker-compose.yml'

        with open(fn, 'w') as f:
            f.write(anyconfig.dumps(config, 'yaml'))

        execute(f'docker-compose -f "{fn}" up -d --no-build')

        remove_path(f'data/{id}/')

    def stop(self, deployment):
        for id in self._get_docker_ids(deployment):
            execute(f'docker stop {id}')
            execute(f'docker rm {id}')

    def _get_docker_ids(self, deployment):
        id = deployment['id']
        ids = execute(f'docker ps -q -f name={id}*').split('\n')
        ids = [id for id in ids if id]
        return ids


def test_docker_controller():
    import anyconfig
    import time
    from utils_ak.loguru import logger, configure_loguru_stdout
    configure_loguru_stdout('DEBUG')

    deployment = anyconfig.load('../../example/deployment.yml')
    ctrl = DockerController()
    ctrl.start(deployment)
    ids = ctrl._get_docker_ids(deployment)
    time.sleep(3)
    for id in ids:
        logger.debug('Logs for id', id=id, logs=execute(f'docker logs {id}'))

    ctrl.stop(deployment)


if __name__ == '__main__':
    test_docker_controller()