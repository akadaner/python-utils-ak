import os
import time
import fire
from loguru import logger


def main(name=None, run_forever=True):
    name = name or os.environ.get('NAME') or 'World'
    logger.info(f'Hello {name}!')
    if run_forever:
        while True:
            time.sleep(1)


if __name__ == '__main__':
    fire.Fire(main)