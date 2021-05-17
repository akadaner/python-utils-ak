import os
from omegaconf import OmegaConf

from utils_ak.job_orchestrator.config import Config


CONFIG_FILES = [
    "configs/.settings.local.yaml",
]

CONFIG_FILES = [os.path.join(os.path.dirname(__file__), fn) for fn in CONFIG_FILES]
omega_confs = [OmegaConf.load(fn) for fn in CONFIG_FILES]
omega_conf = OmegaConf.merge(*omega_confs)
config_dict = OmegaConf.to_container(omega_conf)

config = Config.parse_obj(config_dict)
