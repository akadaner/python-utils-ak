import sys
import math
import json

import stackprinter
from loguru import logger


def patch_serialized_generic_extra(record):
    record["extra"]["serialized"] = serialize(record)


def patch_trace(record):
    exception_info = sys.exc_info()
    if exception_info[0]:
        record["extra"]["stack"] = stackprinter.format(record["exception"])
        record["extra"]["error"] = record["extra"]["stack"].split("\n")[-1]
