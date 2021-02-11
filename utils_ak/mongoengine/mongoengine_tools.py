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


def cast_model(obj, cls):
    if isinstance(obj, ObjectId):
        return cast_model({'_id': obj}, cls)
    elif isinstance(obj, dict):
        if '_id' in obj:
            # fetch and return updated version from server
            element = cls.objects(pk=obj['_id']).first()
            if not element:
                raise Exception('Object not found')
            db_obj = cast_model(element, cls)
            pk = obj['_id']
            d1, d2 = db_obj.to_mongo(), dict(obj)
            d1.pop('_id', None), d2.pop('_id', None)
            return cls(pk=pk, **update_dic(d1, d2))
        else:
            # init
            return cls(**obj)
    elif isinstance(obj, cls):
        return obj
    else:
        raise Exception('Unknown model format')


def cast_dict(obj, cls):
    model = cast_model(obj, cls=cls)
    res = dict(model.to_mongo())
    res['_cls'] = cls.__name__
    return res
