import sys
from decimal import Decimal
from datetime import datetime
import os

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


def cast_js(js_obj, *args, **kwargs):
    if isinstance(js_obj, str):
        return js_obj
    else:
        return json.dumps(js_obj, cls=JsonEncoder, ensure_ascii=False, *args, **kwargs)


def cast_dict_or_list(js_obj, *args, **kwargs):
    if isinstance(js_obj, (dict, list)):
        return js_obj

    # load object from file if path exists
    if isinstance(js_obj, str):
        if os.path.exists(js_obj):
            with open(js_obj, 'r') as f:
                js_obj = f.read()

    try:
        res = json.loads(js_obj, *args, **kwargs)
        if isinstance(res, (dict, list)):
            return res
    except:
        pass

    raise Exception('Unknown type')


dumps = cast_js
loads = cast_dict_or_list




if __name__ == '__main__':
    print(dumps({'a': 'foo'}))
    print(loads(dumps({'a': 'foo', 'dec': Decimal('10.1'), 'today': datetime.now()})))
    print(cast_dict_or_list('columns'))
    print(cast_js(None))

    import apache_beam.transforms.window