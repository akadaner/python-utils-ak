import anyconfig
import copy
import os
import tempfile
from loguru import logger
from utils_ak.deployment.config import BASE_DIR
from utils_ak.deployment.controller import Controller
from utils_ak.coder import cast_js, cast_dict_or_list
from utils_ak.os import *
from utils_ak.dict import fill_template

# todo: success and failure responses (or errors?)


class DockerController(Controller):
    def start(self, deployment):
        id = deployment["id"]

        assert (
            len(deployment["containers"]) == 1
        ), "Only one-container pods are supported for now"

        entity, container = list(deployment["containers"].items())[0]
        params = {"entity": entity, "deployment_id": id, "image": container["image"]}

        config = cast_dict_or_list(
            os.path.join(BASE_DIR, "example/docker-compose.yml.template")
        )
        config = fill_template(config, **params)

        for k, v in container["command_line_arguments"].items():
            config["services"][entity]["command"].append(f"--{k}")
            config["services"][entity]["command"].append(cast_js(v))

        makedirs(f"data/docker-compose/{id}/")
        fn = f"data/docker-compose/{id}/docker-compose.yml"
        with open(fn, "w") as f:
            f.write(anyconfig.dumps(config, "yaml"))

        execute(f'docker-compose -f "{fn}" up -d --no-build')

    def stop(self, deployment_id):
        for id in self._get_docker_ids(deployment_id):
            execute(f"docker stop {id}")
            execute(f"docker rm {id}")
            # todo: review
            execute(f"docker network prune -f")  # clean networks
        remove_path(f"data/docker-compose/{deployment_id}/")

    def _get_docker_ids(self, deployment_id):
        ids = execute(f"docker ps -q -f name={deployment_id}*").split("\n")
        ids = [id for id in ids if id]
        return ids

    def log(self, deployment_id):
        ids = self._get_docker_ids(deployment_id)
        for id in ids:
            logger.debug("Logs for id", id=id, logs=execute(f"docker logs {id}"))


def test_docker_controller():
    import anyconfig
    import time
    from utils_ak.loguru import logger, configure_loguru_stdout

    configure_loguru_stdout("DEBUG")

    deployment = anyconfig.load("../../example/deployment.yml")
    ctrl = DockerController()
    ctrl.start(deployment)
    time.sleep(3)
    ctrl.log(deployment["id"])
    ctrl.stop(deployment["id"])


if __name__ == "__main__":
    test_docker_controller()
