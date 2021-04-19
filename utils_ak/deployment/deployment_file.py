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


if __name__ == "__main__":
    test_gen_deployment()
