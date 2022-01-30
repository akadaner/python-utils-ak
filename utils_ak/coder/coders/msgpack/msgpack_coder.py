import codecs

import msgpack
import decimal
import datetime as datetime_module

from utils_ak.coder import Coder

from utils_ak.coder.binary import (
    pack_datetime,
    pack_decimal,
    pack_time,
    unpack_datetime,
    unpack_decimal,
    unpack_time,
    pack_date,
    unpack_date,
)

EXT_DATE = 1
EXT_DATETIME = 2
EXT_DECIMAL = 3
EXT_TIME = 4


def default(obj):
    if isinstance(obj, datetime_module.datetime):
        return msgpack.ExtType(EXT_DATETIME, pack_datetime(obj))
    elif isinstance(obj, datetime_module.date):
        return msgpack.ExtType(EXT_DATE, pack_date(obj))
    elif isinstance(obj, datetime_module.time):
        return msgpack.ExtType(EXT_TIME, pack_time(obj))
    elif isinstance(obj, decimal.Decimal):
        return msgpack.ExtType(EXT_DECIMAL, pack_decimal(obj))
    else:
        # Wuh-woh
        raise TypeError(
            u"Cannot encode value of type {} to MessagePack: {}".format(
                type(obj).__name__, obj
            )
        )


def ext_hook(code, data):
    """
    Decodes our custom extension types
    """
    if code == EXT_DATETIME:
        return unpack_datetime(data)
    elif code == EXT_DATE:
        return unpack_date(data)
    elif code == EXT_TIME:
        return unpack_time(data)
    elif code == EXT_DECIMAL:
        return unpack_decimal(data)
    else:
        raise TypeError(
            u"Cannot decode unknown extension type {} from MessagePack".format(code)
        )


class MsgPackCoder(Coder):
    def encode(self, obj):
        return msgpack.dumps(obj, default=default, use_bin_type=True)

    def decode(self, obj):
        return msgpack.loads(obj, ext_hook=ext_hook, raw=False)


if __name__ == "__main__":
    coder = MsgPackCoder()
    print(coder.encode({'asdf': 123}))