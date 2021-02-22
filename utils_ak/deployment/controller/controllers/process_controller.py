import anyconfig
import copy
import os
import tempfile
from loguru import logger
from utils_ak.deployment.config import BASE_DIR
from utils_ak.deployment.controller import Controller
from utils_ak.coder import cast_js, cast_dict_or_list
from utils_ak.os import *
from utils_ak.str import cast_unicode
from utils_ak.dict import fill_template
from utils_ak.builtin import *
from utils_ak.deployment.controller.test_controller import test_controller

# todo: add screen support for *nix


class ProcessController(Controller):
    def __init__(self):
        self.processes = {}

    def start(self, deployment):
        id = deployment["id"]

        assert (
            len(deployment["containers"]) == 1
        ), "Only one-container pods are supported for now"

        entity, container = delistify_single(deployment["containers"].items())
        main_file_path = container["main_file_path"]
        command_line_arguments = {}
        for k, v in container["command_line_arguments"].items():
            command_line_arguments[k] = v

        cmd = f'python "{main_file_path}"'
        for k, v in command_line_arguments.items():
            cmd += f" --{k} "
            if isinstance(v, str):
                cmd += v
            elif isinstance(v, (dict, list)):
                js = cast_js(v)
                js = js.replace('"', r"\"")
                js = f'"{js}"'
                cmd += js
            else:
                raise Exception("Unknown command line argument type")

        self.processes[id] = execute(cmd, is_async=True)

    def stop(self, deployment_id):
        self.processes[deployment_id].kill()
        self.processes.pop(deployment_id)

    def log(self, deployment_id):
        pass


if __name__ == "__main__":
    test_controller(ProcessController)
