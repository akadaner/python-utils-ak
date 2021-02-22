import decimal
import datetime
import os
import yaml
import json
import ujson

from io import StringIO

from utils_ak.coder.coder import Coder
from utils_ak.numeric import is_int, is_float
from utils_ak.builtin import delistify


# todo: move to ujson for encoding
# todo: support proper decoding


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return obj.to_json()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return str(obj)
        elif isinstance(obj, datetime.date):
            return str(obj)
        elif isinstance(obj, datetime.time):
            return str(obj)
        elif is_int(obj):
            return int(obj)
        elif is_float(obj):
            return float(obj)
        else:
            return super().default(obj)


class JsonCoder(Coder):
    def __init__(self, encoding="utf-8", indent=None):
        self.encoding = encoding
        self.indent = indent

    def encode(self, obj):
        if not isinstance(obj, str):
            obj = json.dumps(obj, cls=JsonEncoder, ensure_ascii=False)

        if self.encoding:
            obj = obj.encode(self.encoding)

        return obj

    def decode(self, s):
        return ujson.loads(s)


json_coder = JsonCoder(encoding=None)


def cast_js(js_obj):
    return json_coder.encode(js_obj)


def cast_dict_or_list(obj, *args, **kwargs):
    if isinstance(obj, (dict, list)):
        return obj

    if isinstance(obj, bytes):
        try:
            obj = obj.decode("utf-8")
        except:
            raise Exception("Unknown bytes encoding")

    # load object from file if path exists
    if isinstance(obj, str):
        if os.path.exists(obj):
            with open(obj, "r") as f:
                obj = f.read()

        try:
            res = json_coder.decode(obj)
            assert isinstance(res, (dict, list))
            return res
        except:
            pass

        # todo: hardcode, put in different place
        try:
            res = delistify(list(yaml.load_all(StringIO(obj), Loader=yaml.FullLoader)))
            assert isinstance(res, (dict, list))
            return res
        except:
            pass

    raise Exception("Unknown type")


if __name__ == "__main__":
    from utils_ak.coder import test_coder

    test_coder(JsonCoder(), assert_equal=False)
