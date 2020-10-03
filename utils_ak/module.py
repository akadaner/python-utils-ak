""" Python module-level functionality. """
from functools import reduce
import importlib
import git
import glob
import os
import inspect
import sys


def extract_class(src, class_name):
    module = cast_module(src)
    return reduce(getattr, class_name.split("."), module)


extract_class_by_module_name = extract_class


def extract_all_classes(src):
    module = cast_module(src)
    return dict([(name, cls) for name, cls in module.__dict__.items() if isinstance(cls, type)])


def load_module(module_fn, module_name=None):
    if not module_name:
        module_name = module_fn
    spec = importlib.util.spec_from_file_location(module_name, module_fn)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo


def cast_module(obj):
    if inspect.ismodule(obj):
        return obj
    elif isinstance(obj, str):
        if os.path.exists(obj):
            return load_module(obj)
        elif obj in sys.modules:
            return sys.modules[obj]
        else:
            raise Exception(f'Module not found {obj}')
    else:
        raise Exception('Object is not a module or alike')
