import os
import anyconfig
import collections
import yaml
import sys

from utils_ak.dict import dotdict
from utils_ak.builtin import update_dic

import yaml


def load_yaml(fn):
    with open(fn, 'r') as f:
        return yaml.load(f)


# will try to find these in directory with config.py file
BASE_CONFIGS = ['common_config.yml', 'secret_config.yml', 'instance_config.yml']


def cast_config(obj, required=False):
    if obj is None:
        return {}
    elif isinstance(obj, collections.abc.Mapping):
        return obj
    elif isinstance(obj, str):
        if os.path.exists(obj):
            return anyconfig.load(obj)
        else:
            if required:
                raise Exception('Config file not found {}'.format(obj))
            else:
                return {}
    elif isinstance(obj, list):
        res = {}
        for v in obj:
            anyconfig.merge(res, cast_config(v), ac_merge=anyconfig.MS_DICTS)
        return res
    else:
        raise Exception('Unknown config type')


def get_config(configs=None, require_local=False):
    cur_dir = os.getcwd()
    local_config_fn = os.path.splitext(os.path.abspath(sys.argv[0]))[0] + '.yml'

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if not os.path.exists(local_config_fn):
        if require_local:
            raise Exception(f'Local config not found {local_config_fn}')
        local_config_fn = None

    base_configs = [os.path.join(os.getcwd(), base_config) for base_config in BASE_CONFIGS]

    configs = configs or []
    configs = [os.path.join(cur_dir, config) for config in configs]

    os.chdir(cur_dir)
    res = cast_config(base_configs + [local_config_fn] + configs)
    # return dotdict(res) # todo: dotdict has a bug with pickle and threading
    return res

config = get_config()


if __name__ == '__main__':
    print(config)