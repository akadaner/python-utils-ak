import sys
import math
import stackprinter
from utils_ak.coder import json_coder
from loguru import logger


def format_as_json(record):
    assert "_json" not in record["extra"]

    extra = dict(record["extra"])
    extra.pop("source", None)

    simplified = {
        "level": record["level"].name,
        "message": record["message"],
        "ts": int(math.floor(record["time"].timestamp() * 1000)),  # epoch millis
        "inner_source": record["extra"].get("source", ""),
        "extra": extra,
        "stack": "",
        "error": "",
    }

    exception_info = sys.exc_info()
    if exception_info[0]:
        simplified["stack"] = stackprinter.format(record["exception"])
        simplified["error"] = simplified["stack"].split("\n")[-1]

    record["extra"]["_json"] = json_coder.encode(simplified)
    return "{extra[_json]}\n"


def format_with_trace(record):
    format = "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>"

    exception_info = sys.exc_info()
    if exception_info[0]:
        for key in ["_stack", "_error", "_original_extra"]:
            assert key not in record["extra"]

        assert "_stack" not in record["extra"] and "_error" not in record["extra"]
        record["extra"]["_stack"] = stackprinter.format(record["exception"])
        record["extra"]["_error"] = record["extra"]["_stack"].split("\n")[-1]
        original_extra = dict(record["extra"])
        original_extra.pop("_stack")
        original_extra.pop("_error")
        record["extra"]["_original_extra"] = original_extra
        format += " | <yellow>{extra[_original_extra]}</yellow> | <red>{extra[_error]}</red>\n<red>{extra[_stack]}</red>\n"
    else:
        format += " | <yellow>{extra}</yellow>\n"
    return format


def configure_loguru_stdout(
    level="DEBUG", remove_others=True, formatter=format_with_trace
):
    if remove_others:
        logger.remove()

    logger.add(
        sys.stdout,
        level=level,
        format=formatter,
    )


def test():
    configure_loguru_stdout(formatter=format_as_json)

    logger.info("Info message", foo="bar")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("Oups...")

    configure_loguru_stdout(formatter=format_with_trace)
    logger.info("Info message", foo="bar")

    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("Oups...")


if __name__ == "__main__":
    test()
