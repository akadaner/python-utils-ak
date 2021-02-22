import os
import time
import fire
from loguru import logger
from notifiers import get_notifier
from datetime import datetime


def main(name=None, run_forever=True, beep=False):
    telegram_bot_token = "<telegram_bot_token>"
    telegram = get_notifier("telegram")

    name = name or os.environ.get("NAME") or "World"
    logger.info(f"Hello {name}!")

    if beep:
        for i in range(5):
            telegram.notify(
                message=f"Hi! from {datetime.now()}",
                token=telegram_bot_token,
                chat_id=160773045,
            )
            time.sleep(2)

    if run_forever:
        while True:
            time.sleep(2)


if __name__ == "__main__":
    fire.Fire(main)
