import subprocess

from utils_ak.str import cast_unicode


def execute(cmd, is_async=False):
    if is_async:
        process = subprocess.Popen(cmd, shell=True)
    else:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        return ' '.join([cast_unicode(x) for x in [output, error] if x])