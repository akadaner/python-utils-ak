from bson.objectid import ObjectId
from mongoengine import Document


def cast_object_id(obj):
    if isinstance(obj, ObjectId):
        return obj
    elif isinstance(obj, str):
        return ObjectId(obj)
    elif obj is None:
        return None
    elif isinstance(obj, dict):
        return obj["_id"]
    else:
        raise Exception("Unknown object id type")


def cast_model(obj, cls):
    if isinstance(obj, ObjectId) or isinstance(obj, str):
        object_id = cast_object_id(obj)
        return cast_model({"_id": object_id}, cls)
    elif isinstance(obj, dict):
        if "_id" in obj:
            # fetch and return updated version from server
            element = cls.objects(pk=obj["_id"]).first()
            if not element:
                raise Exception("Object not found")
            return element
        else:
            # init
            return cls(**obj)
    elif isinstance(obj, cls):
        return obj
    else:
        raise Exception("Unknown model format")


def cast_dict(obj):
    if isinstance(obj, dict):
        return obj
    elif isinstance(obj, Document):
        res = dict(obj.to_mongo())
        res["_cls"] = obj.__class__.__name__
        return res
    else:
        raise Exception("Unknown dict type")
