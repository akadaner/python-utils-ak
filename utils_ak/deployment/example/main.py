import os
import time
import fire
from loguru import logger
from notifiers import get_notifier
from datetime import datetime

# todo: make beep properly

from utils_ak.deployment.example.config import settings


def main(name=None, run_forever=True, beep=True):
    telegram = get_notifier("telegram")

    name = name or os.environ.get("NAME") or "World"
    logger.info(f"Hello {name}!")

    if beep:
        for i in range(5):
            telegram.notify(
                message=f"Hi! from {datetime.now()}",
                token=settings["telegram_bot_token"],
                chat_id=settings["telegram_chat_id"],
            )
            time.sleep(2)

    if run_forever:
        while True:
            time.sleep(2)


if __name__ == "__main__":
    fire.Fire(main)
