import os
from utils_ak.coder import *
from utils_ak.dict import *


def create_deployment(
    container_name, deployment_id, payload, image=None, python_main=None
):
    deployment_template = cast_dict_or_list(
        os.path.join(os.path.dirname(__file__), "deployment.yml.template")
    )

    kwargs = {
        "container_name": container_name,
        "deployment_id": str(deployment_id),
        "payload": payload,
        "image": image or "",
        "python_main": python_main or "",
    }
    deployment = fill_template(deployment_template, **kwargs)
    return deployment
