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
        # create docker-compose file and run it without building

        dcc = {'version': '3', 'services': {}}  # docker compose config
        for i, (name, container) in enumerate(deployment['containers'].items()):
            service = {'image': container['image'], 'tty': True, 'container_name': f'{deployment["id"]}_{i}'}

            service['command'] = []

            for k, v in container['command_line_arguments'].items():
                service['command'].append(f'--{k}')
                service['command'].append(cast_js(v))
            dcc['services'][name] = service

        makedirs(f'data/{deployment["id"]}/')
        fn = f'data/{deployment["id"]}/docker-compose.yml'

        with open(fn, 'w') as f:
            f.write(anyconfig.dumps(dcc, 'yaml'))

        execute(f'docker-compose -f "{fn}" up -d --no-build')

        remove_path(f'data/{deployment["id"]}/')

    def stop(self, deployment):
        id = deployment['id']

        ids = execute(f'docker ps -q -f name={id}*').split('\n')
        ids = [id for id in ids if id]

        for id in ids:
            execute(f'docker stop {id}')
            execute(f'docker rm {id}')
