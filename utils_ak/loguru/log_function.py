import functools
from loguru import logger
from utils_ak.loguru import configure_loguru_stdout


def log_function(*, entry=True, exit=True, level="DEBUG"):
    def wrapper(func):
        name = func.__name__

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            logger_ = logger.opt(depth=1)
            if entry:
                logger_.log(
                    level,
                    "Entering function",
                    function_name=name,
                    args=args,
                    kwargs=kwargs,
                )
            result = func(*args, **kwargs)
            if exit:
                logger_.log(
                    level, "Exiting function", function_name=name, result=result
                )
            return result

        return wrapped

    return wrapper


def test():
    configure_loguru_stdout("DEBUG")

    @log_function()
    def f(*args, **kwargs):
        logger.info("Inside the function")
        return 1

    f("a", b=1)


if __name__ == "__main__":
    test()
