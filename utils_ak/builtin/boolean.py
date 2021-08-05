def cast_bool(obj):
    if isinstance(obj, bool):
        return obj
    elif isinstance(obj, str):
        if obj.lower() in ["y", "true"]:
            return True
        elif obj.lower() in ["n", "false"]:
            return False
        else:
            raise Exception("Unknown format: {} {}".format(type(obj), obj))
    else:
        raise Exception("Unknown format: {} {}".format(type(obj), obj))
