import anyconfig
from utils_ak.coder import *
from utils_ak.builtin import *
import fire


def generate_bats_from_deployment(deployment_fn="sample_deployment.yml"):
    d = anyconfig.load(deployment_fn)

    assert (
        len(list(d["containers"])) == 1
    ), "Only single container allowed at the moment"

    container = iter_get(d["containers"].values())
    config_js = cast_js(container["command_line_arguments"]["config"])
    config_js = config_js.replace('"', r"\"")
    config_js = f'"{config_js}"'

    image = container["image"]

    with open("build.bat", "w") as f:
        f.write(f"docker build -t {image}:latest . --no-cache\n")
        f.write("pause")

    with open("push.bat", "w") as f:
        f.write(f"docker push {image}:latest\n")
        f.write("pause")

    with open("run.bat", "w") as f:
        f.write(f"""docker run {image} --config {config_js}\n""")
        f.write("pause")


if __name__ == "__main__":
    fire.Fire(generate_bats_from_deployment)
