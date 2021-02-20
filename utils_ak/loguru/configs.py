import sys
from loguru import logger


def configure_loguru_stdout(level="DEBUG", remove_others=True):
    if remove_others:
        logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level> - <yellow>{extra}</yellow>",
    )
