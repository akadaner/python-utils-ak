""" Python reflexion-level functionality. """
from functools import reduce
import importlib
import inspect
import sys


def extract_class(src, class_name):
    module = load(src)
    return reduce(getattr, class_name.split("."), module)


extract_class_by_module_name = extract_class


def extract_all_classes(src):
    module = load(src)
    return dict([(name, cls) for name, cls in module.__dict__.items() if isinstance(cls, type)])


def load(module_obj, module_name=None, reload=False, import_globals=False, globals_dic=None):
    """
    :param import_globals: does not work for dependent modules properly. Use with ultimate care
    :return:
    """
    if inspect.ismodule(module_obj):
        module = module_obj
        if reload:
            module = importlib.reload(module)
    elif isinstance(module_obj, str):
        module_name = module_name or module_obj
        module = sys.modules.get(module_name)

        if not module or reload:
            spec = importlib.util.spec_from_file_location(module_name, module_obj)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
    else:
        raise Exception('Unknown module object')

    # todo: make better implementation
    # put all module variables into globals to reload
    globals_dic = globals_dic or globals()
    if import_globals:
        for x in dir(module):
            if not x.startswith('_'):
                globals_dic[x] = getattr(module, x)
    return module

cast_module = load
