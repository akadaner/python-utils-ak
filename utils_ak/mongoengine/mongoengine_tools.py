from utils_ak import update_dic

from bson.objectid import ObjectId


def cast_object_id(obj):
    if isinstance(obj, ObjectId):
        return obj
    elif isinstance(obj, str):
        return ObjectId(obj)
    elif obj is None:
        return None
    elif isinstance(obj, dict):
        return obj['_id']
    else:
        raise Exception('Unknown object id type')


def cast_model(fs_obj, cls):
    if isinstance(fs_obj, ObjectId):
        return cast_model({'_id': fs_obj}, cls)
    elif isinstance(fs_obj, dict):
        if '_id' in fs_obj:
            # fetch and return updated version from server
            db_fs = cast_model(cls.objects(pk=fs_obj['_id']).first(), cls)
            pk = fs_obj['_id']
            d1, d2 = dict(db_fs), dict(fs_obj)
            d1.pop('_id', None), d2.pop('_id', None)
            return cls(pk=pk, **update_dic(d1, d2))
        else:
            # init
            return cls(**fs_obj)
    elif isinstance(fs_obj, cls):
        return fs_obj
    else:
        raise Exception('Unknown model format')


def cast_dict(fs_obj, cls=None):
    model = cast_model(fs_obj, cls=None)
    return dict(model.to_mongo())
