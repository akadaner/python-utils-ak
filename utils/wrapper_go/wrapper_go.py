import subprocess
import os


def go_build(path, force_rebuild=False):
    filename = os.path.splitext(path)[0]
    rebuild_flag = "-a" if force_rebuild else ""
    command = f"go build {rebuild_flag} -o {filename} {path}"
    print(command)
    subprocess.call(command, shell=True)


def go_run(path, verbatim=True, **kwargs):
    filename = os.path.splitext(path)[0]
    command = f"{filename}"
    for k, v in kwargs.items():
        arg = f" -{k}=\"{v}\""
        command += arg
    print(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if verbatim:
        while True:
            line = process.stdout.readline().rstrip()
            if not line:
                break
            print(line.decode("utf-8"))


if __name__ == "__main__":
    go_build("/mnt/granular_storage/backoffice.go")
    go_run("/mnt/granular_storage/backoffice.go")