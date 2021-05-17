from pydantic import *


class Config(BaseSettings):
    EMAIL_USER: str
    EMAIL_PSWD: str

    class Config:
        env_file = ".env"
        case_sensitive = True


config = Config()
