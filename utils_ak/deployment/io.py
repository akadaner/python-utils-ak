import anyconfig
from utils_ak.coder import *


def read_deployment(deployment_fn):
    deployment = anyconfig.load(deployment_fn)
    deployment_dirname = os.path.abspath(os.path.dirname(deployment_fn))
    for container_name, container in deployment["containers"].items():
        if not os.path.exists(container["python_main"]):
            possible_local_fn = os.path.join(
                deployment_dirname, container["python_main"]
            )
            if os.path.exists(possible_local_fn):
                container["python_main"] = possible_local_fn
    return deployment
