import datetime
import decimal
import struct
import six

STRUCT_DATE = struct.Struct('!HBB')
STRUCT_DATETIME = struct.Struct('!q')
STRUCT_DECIMAL_LENGTH = struct.Struct('!H')
STRUCT_TIME = struct.Struct('!3BL')


def pack_date(obj):
    return STRUCT_DATE.pack(obj.year, obj.month, obj.day)


def unpack_date(data):
    return datetime.date(*STRUCT_DATE.unpack(data))


def pack_datetime(obj):
    if obj.tzinfo is not None:
        raise TypeError(u'Cannot encode time zone-aware date-times to MessagePack')
    seconds = (obj - datetime.datetime(1970, 1, 1)).total_seconds()
    microseconds = int(seconds * 1000000.0)
    return STRUCT_DATETIME.pack(microseconds)


def unpack_datetime(data):
    microseconds = STRUCT_DATETIME.unpack(data)[0]
    return datetime.datetime.utcfromtimestamp(microseconds / 1000000.0)


def pack_time(obj):
    return STRUCT_TIME.pack(obj.hour, obj.minute, obj.second, obj.microsecond)


def unpack_time(data):
    return datetime.time(*STRUCT_TIME.unpack(data))


def pack_decimal(obj):
    obj_str = six.text_type(obj)[:65535].encode('utf-8')
    obj_len = len(obj_str)
    obj_encoder = struct.Struct(f'!H{obj_len}s')
    return obj_encoder.pack(obj_len, obj_str)


def unpack_decimal(data):
    obj_len = STRUCT_DECIMAL_LENGTH.unpack(data[:2])[0]
    obj_decoder = struct.Struct(f'!{obj_len}s')
    return decimal.Decimal(obj_decoder.unpack(data[2:])[0].decode('utf-8'))
