import anyconfig
from utils_ak.coder import *

d = anyconfig.load(r"sample_deployment.yml")

config_js = cast_js(d["containers"]["test-worker"]["command_line_arguments"]["config"])
config_js = config_js.replace('"', r"\"")
config_js = f'"{config_js}"'


with open("build.bat", "w") as f:
    f.write("docker build -t akadaner/test-worker:latest . --no-cache")
    f.write("pause")

with open("run.bat", "w") as f:
    f.write(f"""docker run akadaner/test-worker --config {config_js}\n""")
    f.write("pause")
