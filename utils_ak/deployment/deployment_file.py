import anyconfig
import os

from utils_ak.builtin import *
from utils_ak.coder import *
from utils_ak.dict import fill_template


def gen_deployment(deployment_id, payload, image=None, python_main=None):
    deployment = cast_dict_or_list(
        os.path.join(os.path.dirname(__file__), "deployment.yml.template")
    )

    kwargs = {
        "deployment_id": str(deployment_id),
        "payload": payload,
        "image": image or "",
        "python_main": python_main or "",
    }
    deployment = fill_template(deployment, **kwargs)
    return deployment


def test_gen_deployment():
    print(gen_deployment("<deployment_id>", "<payload>", "<image>", "<python_main>"))


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


if __name__ == "__main__":
    test_gen_deployment()
