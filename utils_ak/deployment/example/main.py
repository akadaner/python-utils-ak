import os
import time
import fire
import notifiers

from loguru import logger
from datetime import datetime, timedelta

from utils_ak.deployment.example.config import config


def main(name=None, run_forever=False):
    notifier = notifiers.get_notifier("gmail")

    name = name or os.environ.get("NAME") or "World"
    logger.info(f"Hello {name}!")

    for i in range(5):
        notifier.notify(
            message=f"Hello, Friend! {datetime.now()}",
            subject="<subject>",
            to=config.EMAIL_USER,
            username=config.EMAIL_USER,
            password=config.EMAIL_PSWD,
        )
        time.sleep(2)

    if run_forever:
        while True:
            time.sleep(2)


if __name__ == "__main__":
    fire.Fire(main)
