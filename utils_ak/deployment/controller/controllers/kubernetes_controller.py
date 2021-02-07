import anyconfig
import copy
import os
import tempfile
from utils_ak.deployment.controller import Controller
from utils_ak.serialization import cast_js
from utils_ak.os import *


class KubernetesController(Controller):
    def start(self, deployment):
        id = deployment['id']

        # create kubernetes file

        res = ''

        kc = {'apiVersion'}

        dcc = {'version': '3', 'services': {}}  # docker compose config
        for i, (name, container) in enumerate(deployment['containers'].items()):
            service = {'image': container['image'], 'tty': True, 'container_name': f'{id}_{i}'}

            service['command'] = []

            for k, v in container['command_line_arguments'].items():
                service['command'].append(f'--{k}')
                service['command'].append(cast_js(v))
            dcc['services'][name] = service

        makedirs(f'data/{id}/')
        fn = f'data/{id}/docker-compose.yml'

        with open(fn, 'w') as f:
            f.write(anyconfig.dumps(dcc, 'yaml'))

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

    deployment = anyconfig.load('../../examples/hello-world.yml')
    ctrl = DockerController()
    ctrl.start(deployment)
    ids = ctrl._get_docker_ids(deployment)
    time.sleep(3)
    for id in ids:
        logger.debug('Logs for id', id=id, logs=execute(f'docker logs {id}'))

    ctrl.stop(deployment)


if __name__ == '__main__':
    test_docker_controller()