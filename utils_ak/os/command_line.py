import subprocess
from loguru import logger

from utils_ak.str import cast_unicode


def execute(cmd, is_async=False, verbose=True):
    if verbose:
        logger.debug("Executing command", cmd=cmd, is_async=is_async)
    if is_async:
        subprocess.Popen(cmd, shell=True)
    else:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        output, error = process.communicate()
        res = " ".join([cast_unicode(x) for x in [output, error] if x])
        if verbose:
            logger.debug("Execution response", response=res)
        return res
