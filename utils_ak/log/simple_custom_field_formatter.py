import logging
import json
from utils_ak.serialization.serializer import JsonSerializer


class SimpleCustomFieldFormatter(logging.Formatter):
    def __init__(self, fmt=None, msg_fmt="{msg} {custom}", encoder=None):
        """
        :param fmt:
        :param msg_fmt: string pattern for "msg" and "extra" fields.
        """
        self.msg_fmt = msg_fmt
        self.encoder = encoder or JsonSerializer()
        super().__init__(fmt)

    def format(self, record):
        custom = getattr(record, 'custom', {})
        record.msg = self.msg_fmt.format(**{'msg': record.msg, 'custom': self.encoder.encode(custom)})
        return super().format(record)
