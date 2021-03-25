from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    settings_files=[
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/2020.09-qset/qset-feature-store/qset_feature_store/configs/default_settings.yaml",
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/2020.09-qset/qset-feature-store/qset_feature_store/configs/settings.yaml",
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/2020.09-qset/qset-feature-store/qset_feature_store/configs/settings.local.yaml",
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/2020.09-qset/qset-feature-store/qset_feature_store/configs/worker_settings.yaml",
        "/Users/arsenijkadaner/Yandex.Disk.localized/master/code/git/2020.09-qset/qset-feature-store/qset_feature_store/configs/.secrets.yaml",
    ],
    environments=True,
    load_dotenv=True,
    # load_dotenv={"when": {"env": {"is_in": ["development"]}}},
    envvar_prefix="DYNACONF",
    env_switcher="ENV_FOR_DYNACONF",
    dotenv_path="configs/.env",
)


def test():
    from pprint import pprint

    with settings.using_env("default"):
        pprint(settings.as_dict())
    pprint(settings.as_dict())


if __name__ == "__main__":
    test()
