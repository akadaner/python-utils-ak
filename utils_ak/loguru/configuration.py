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


def time_formatter(record):
    return "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green>"


def level_formatter(record):
    return "<level>{level: <8}</level>"


def name_formatter(record):
    return "<cyan>{name}</cyan>"


def message_formatter(record):
    return "<level>{message}</level>"


def module_formatter(record):
    return "<cyan>{module}</cyan>"


def extra_formatter(record, fancy=True):
    if fancy:

        values = []
        for i, (k, v) in enumerate(record["extra"]["_extra"].items()):
            values.append(
                ": ".join(
                    [
                        f"<magenta>{{extra[_extra_{2 * i}]}}</magenta>",
                        f"<yellow>{{extra[_extra_{2 * i + 1}]}}</yellow>",
                    ]
                )
            )
            record["extra"][f"_extra_{2 * i}"] = k
            record["extra"][f"_extra_{2 * i + 1}"] = v
        return ", ".join(values)

    return "<yellow>{extra[_extra]}</yellow>"


def exception_formatter(record):
    if not record["exception"]:
        return

    assert all(key not in record["extra"] for key in ["_stack", "_error"])

    record["extra"]["_stack"] = _get_stack(record["exception"])
    record["extra"]["_error"] = record["extra"]["_stack"].split("\n")[-1]
    _extra = dict(record["extra"])
    _extra.pop("_stack")
    _extra.pop("_error")
    record["extra"]["_extra"] = _extra
    return "<red>{extra[_error]}</red>\n<red>{extra[_stack]}</red>"


def format_with_trace(
    record,
    formatters=(
        time_formatter,
        module_formatter,
        message_formatter,
        extra_formatter,
        exception_formatter,
    ),
    separator=" | ",
):
    # workaround for custom extra
    record["extra"]["_extra"] = dict(record["extra"])
    values = [formatter(record) for formatter in formatters]
    values = [value for value in values if value]
    return separator.join(values) + "\n"


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
