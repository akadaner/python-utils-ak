from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    settings_files=[
        "configs/default_settings.yaml",
        "configs/settings.yaml",
        "configs/settings.local.yaml",
        "configs/.secrets.yaml",
    ],
    environments=True,
    load_dotenv=True,
    envvar_prefix="DYNACONF",
    env_switcher="ENV_FOR_DYNACONF",
    dotenv_path="configs/.env",
)
