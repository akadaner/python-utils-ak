import tempfile
import os
import inspect

from collections import defaultdict
from loguru import logger

from utils_ak.os import makedirs, list_files


class LazyTester:
    def __init__(self):
        self.logs_path = None
        self.app_path = None
        self.buffer = defaultdict(list)  # {source: values}

    def set_logs_path(self, path):
        self.logs_path = path

    def set_app_path(self, path):
        self.app_path = os.path.abspath(path)  # normalized

    def set_function_logs_path(self):
        stack = inspect.stack()[1]
        fn, function_name = stack[1], stack[3]

        assert self.app_path is not None

        # todo: crop better in future python version (3.10)
        local_fn = fn[len(self.app_path) :]
        if local_fn.startswith("/"):
            local_fn = local_fn[1:]
        self.set_logs_path(os.path.join(self.logs_path, local_fn, function_name + "/"))

    def _format_log(self, value, **kwargs):
        log_values = [value]
        for k, v in kwargs.items():
            log_values.append(f"{k}: {v}")
        return " | ".join(log_values)

    def log(self, value, source="default", **kwargs):
        self.buffer[source].append(self._format_log(value, **kwargs))

    def _dump_logs(self, path):
        makedirs(path)
        for source, log_values in self.buffer.items():
            log_path = os.path.join(path, source) + ".log"
            with open(log_path, "w") as f:
                f.write("\n".join(log_values))

    def _assert_equal_directory_contents(self, dir1, dir2):
        if not dir1.endswith("/"):
            dir1 += "/"
        if not dir2.endswith("/"):
            dir2 += "/"

        local_fns1 = [fn[len(dir1) :] for fn in list_files(dir1)]
        local_fns2 = [fn[len(dir2) :] for fn in list_files(dir2)]
        assert set(local_fns1) == set(local_fns2)

        local_fns = local_fns1
        for local_fn in local_fns:
            fn1 = os.path.join(dir1, local_fn)
            fn2 = os.path.join(dir2, local_fn)
            with open(fn1, "r") as f:
                contents1 = f.read()

            with open(fn2, "r") as f:
                contents2 = f.read()
            assert contents1 == contents2

    def assert_logs(self):
        if not os.path.exists(self.logs_path):
            logger.info(f"Logs not found: {self.logs_path}. Initializing...")
            self._dump_logs(self.logs_path)
        else:
            with tempfile.TemporaryDirectory() as temp_dir:
                self._dump_logs(temp_dir)
                self._assert_equal_directory_contents(temp_dir, self.logs_path)


def test_lazy_tester():
    lazy_tester = LazyTester()
    lazy_tester.set_logs_path("tests/lazy_tester_logs/")
    lazy_tester.set_app_path(".")
    lazy_tester.set_function_logs_path()

    lazy_tester.log("This is a test", var="<var>")
    lazy_tester.assert_logs()


if __name__ == "__main__":
    test_lazy_tester()
