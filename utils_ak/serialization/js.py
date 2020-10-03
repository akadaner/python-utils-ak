import sys
from decimal import Decimal
from datetime import datetime

# ultimate json tool for convenient json handling and stuff

# todo: datetime support
# todo: bson.json_util?
# todo: simplejson?

if sys.version_info[:2] == (2, 6):
    # In Python 2.6, json does not include object_pairs_hook. Use simplejson
    # instead.
    try:
        import simplejson as json
    except ImportError:
        import json
else:
    import json


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, datetime):
            return str(obj)
        else:
            return super().default(obj)


def validate(data):
    try:
        return json.loads(data)
    except ValueError:
        pass


def cast_str(js_obj, *args, **kwargs):
    if isinstance(js_obj, str):
        return js_obj
    else:
        return json.dumps(js_obj, cls=JsonEncoder, ensure_ascii=False, *args, **kwargs)


def cast_dict(js_obj, *args, **kwargs):
    if isinstance(js_obj, dict):
        return js_obj
    else:
        return json.loads(js_obj, *args, **kwargs)


dumps = cast_str
loads = cast_dict

if __name__ == '__main__':
    print(dumps({'a': 'фыва'}))
    print(loads(dumps({'a': 'фыва', 'dec': Decimal('10.1'), 'today': datetime.now()})))
