import os
import anyconfig
import collections
import yaml
import sys

from utils_ak.dict import dotdict
from utils_ak.builtin import update_dic


def cast_config(obj, required=False):
    if obj is None:
        if not required:
            return {}
        else:
            raise Exception('Config required')
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


def get_config(configs=None, require_local=False, global_configs=('common_config.yml', 'secret_config.yml', 'instance_config.yml')):
    local_config_fn = os.path.splitext(os.path.abspath(sys.argv[0]))[0] + '.yml'

    if not os.path.exists(local_config_fn):
        if require_local:
            raise Exception(f'Local config not found {local_config_fn}')
        local_config_fn = None

    res = []
    # add global configs
    for base_config in global_configs:
        for dirname in [os.getcwd(), os.path.dirname(os.path.abspath(sys.argv[0]))]:
            fn = os.path.join(dirname, base_config)
            if os.path.exists(fn):
                res.append(fn)
                break

    configs = configs or []
    res += configs

    # add local config
    res.append(local_config_fn)

    return cast_config(res)


def test():
    print(get_config(global_configs=('sample_configs/common_config.yml',
                                     'sample_configs/secret_config.yml',
                                     'sample_configs/instance_config.yml')))


if __name__ == '__main__':
    test()