import sys
import math
import stackprinter
from utils_ak.coder import json_coder
from loguru import logger
import better_exceptions


def _get_stack(exception, engine="better_exceptions"):
    assert engine in ["stackprinter", "better_exceptions"]

    if engine == "better_exceptions":
        return "".join(better_exceptions.format_exception(*exception))
    elif engine == "stackprinter":
        return stackprinter.format(exception)


def format_as_json(record):
    assert "_json" not in record["extra"]

    extra = dict(record["extra"])
    extra.pop("source", None)

    record_dic = {
        "level": record["level"].name,
        "message": record["message"],
        "ts": int(math.floor(record["time"].timestamp() * 1000)),  # epoch millis
        "inner_source": record["extra"].get("source", ""),
        "extra": extra,
        "stack": "",
        "error": "",
    }

    if record["exception"]:
        record_dic["stack"] = _get_stack(record["exception"])
        record_dic["error"] = record_dic["stack"].split("\n")[-1]

    record["extra"]["_json"] = json_coder.encode(record_dic)
    return "{extra[_json]}\n"


def format_with_trace(record):
    format = "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>"

    if record["exception"]:
        assert all(
            key in record["extra"] for key in ["_stack", "_error", "_original_extra"]
        )
        assert all(key not in record["extra"] for key in ["_stack", "_error"])

        record["extra"]["_stack"] = _get_stack(record["exception"])
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
    level="DEBUG",
    remove_others=True,
    formatter=format_with_trace,
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
    a = 1
    b = 0
    try:
        a / b
    except ZeroDivisionError:
        logger.error("Oups...")
        logger.exception("Oups...")

    configure_loguru_stdout(formatter=format_with_trace)
    logger.info("Info message", foo="bar")

    try:
        a / b
    except ZeroDivisionError:
        logger.error("Oups...")
        logger.exception("Oups...")


if __name__ == "__main__":
    test()
