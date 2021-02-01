import sys
import math
import json

import stackprinter
from loguru import logger


def serialize(record):
    extra = dict(record['extra'])
    extra.pop('source', None)

    simplified = {
        'level': record['level'].name,
        'message': record['message'],
        'ts': int(math.floor(record['time'].timestamp() * 1000)),  # epoch millis
        'inner_source': record['extra'].get('source', 'default'),
        'extra': extra
    }

    exception_info = sys.exc_info()
    if exception_info[0]:
        simplified['stack'] = stackprinter.format(record["exception"])
        simplified['error'] = simplified['stack'].split('\n')[-1]
    return json.dumps(simplified)


def patch_serialized_generic_extra(record):
    record['extra']['serialized'] = serialize(record)
