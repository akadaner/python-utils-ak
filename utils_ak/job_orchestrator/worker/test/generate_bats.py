import anyconfig
from utils_ak.coder import *
from utils_ak.builtin import *

d = anyconfig.load(r"sample_deployment.yml")

assert len(list(d["containers"])) == 1, "Only single container allowed at the moment"

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
