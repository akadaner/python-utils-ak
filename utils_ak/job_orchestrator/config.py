from typing import *
from pydantic import *


class Config(BaseSettings):
    MONGODB_HOST: str
    MONGODB_DB: str
    TRANSPORT: List[Any]

    class Config:
        case_sensitive = True
