import datetime
import msgpack
import decimal
from utils_ak.serialization.binary import pack_datetime, pack_decimal, pack_time, unpack_datetime, unpack_decimal, unpack_time, pack_date, unpack_date

EXT_DATE = 1
EXT_DATETIME = 2
EXT_DECIMAL = 3
EXT_TIME = 4


def default(obj):
    if isinstance(obj, datetime.datetime):
        return msgpack.ExtType(EXT_DATETIME, pack_datetime(obj))
    elif isinstance(obj, datetime.date):
        return msgpack.ExtType(EXT_DATE, pack_date(obj))
    elif isinstance(obj, datetime.time):
        return msgpack.ExtType(EXT_TIME, pack_time(obj))
    elif isinstance(obj, decimal.Decimal):
        return msgpack.ExtType(EXT_DECIMAL, pack_decimal(obj))
    else:
        # Wuh-woh
        raise TypeError(u'Cannot encode value of type {} to MessagePack: {}'.format(type(obj).__name__, obj))


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
        raise TypeError(u'Cannot decode unknown extension type {} from MessagePack'.format(code))


class MsgPackSerializer:
    @staticmethod
    def encode(obj):
        return msgpack.dumps(obj, default=default, use_bin_type=True)

    @staticmethod
    def decode(obj):
        return msgpack.loads(obj, ext_hook=ext_hook, raw=False)


if __name__ == '__main__':
    ser = MsgPackSerializer()
    obj = {'int': 1,
           'datetime': datetime.datetime.now(),
           'decimal': decimal.Decimal('0.12431234123000'),
           'date': datetime.date(2018, 1, 1),
           'time': datetime.datetime.now().time()}
    data = ser.encode(obj)
    print(obj)
    print(len(data), data)
    print(ser.decode(data))
