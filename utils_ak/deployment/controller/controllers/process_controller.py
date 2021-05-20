from utils_ak.deployment.controller import Controller
from utils_ak.coder import cast_js, cast_dict_or_list
from utils_ak.os import *
from utils_ak.builtin import *


class ProcessController(Controller):
    """ Controller using processes. """

    def __init__(self):
        self.processes = {}

    def start(self, deployment):
        id = deployment["id"]

        assert (
            len(deployment["containers"]) == 1
        ), "Only one-container pods are supported for now"

        entity, container = delistify_single(deployment["containers"].items())
        python_main_path = container["python_main"]
        command_line_arguments = {}
        for k, v in container["command_line_arguments"].items():
            command_line_arguments[k] = v

        cmd = f'python "{python_main_path}"'
        for k, v in command_line_arguments.items():
            cmd += f" --{k} "
            if isinstance(v, str):
                cmd += v
            elif isinstance(v, (dict, list)):
                js = cast_js(v)
                js = js.replace('"', r"\"")
                js = f'"{js}"'
                cmd += js
            elif isinstance(v, bool):
                cmd += "True"
            else:
                raise Exception("Unknown command line argument type")
        self.processes[id] = execute(cmd, is_async=True)

    def stop(self, deployment_id):
        try:
            self.processes[deployment_id].kill()
            self.processes.pop(deployment_id)
        except:
            pass

    def log(self, deployment_id):
        pass
