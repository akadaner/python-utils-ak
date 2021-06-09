import tempfile
import os
import inspect

from collections import defaultdict
from loguru import logger

from utils_ak.os import makedirs, list_files, remove_path
from utils_ak.str import trim


class LazyTester:
    def __init__(self, verbose=False):
        self.root = "tests/lazy_tester_logs"
        self.app_path = None
        self.function_path = ""
        self.local_path = ""
        self.default_source = "default"
        self.buffer = defaultdict(list)  # {source: values}
        self.verbose = verbose

    def configure(self, root=None, app_path=None, local_path=None, source=None):
        if root:
            self.root = os.path.abspath(root)
        if app_path:
            self.app_path = os.path.abspath(app_path)

        self.local_path = local_path or self.local_path
        self.default_source = source or self.default_source

    def configure_function_path(self):
        assert self.root is not None and self.app_path is not None

        stack = inspect.stack()[1]
        fn, function_name = stack[1], stack[3]
        local_fn = trim(fn, self.app_path + "/")
        self.function_path = os.path.join(local_fn, function_name)

    @property
    def path(self):
        assert self.root is not None
        return os.path.join(self.root, self.function_path, self.local_path)

    def _format_log(self, value, **kwargs):
        log_values = [str(value)]
        for k, v in kwargs.items():
            log_values.append(f"{k}: {str(v)}")
        return " | ".join(log_values)

    def log(self, value, source=None, **kwargs):
        source = source or self.default_source
        self.buffer[source].append(self._format_log(value, **kwargs))
        if self.verbose:
            logger.info(str(value), source=source, **kwargs)

    def _flush(self, path):
        makedirs(path + "/")
        for source, log_values in self.buffer.items():
            log_path = os.path.join(path, source) + ".log"
            with open(log_path, "w") as f:
                f.write("\n".join(log_values))
        self.buffer = defaultdict(list)

    def _assert_equal_directory_contents(self, dir1, dir2):
        dir1 = os.path.abspath(dir1)  # normalize
        dir2 = os.path.abspath(dir2)  # normalize

        local_fns1 = [trim(fn, dir1 + "/") for fn in list_files(dir1)]
        local_fns2 = [trim(fn, dir2 + "/") for fn in list_files(dir2)]

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

    def assert_logs(self, reset=False):
        if reset and os.path.exists(self.path):
            remove_path(self.path)
        if not os.path.exists(self.path):
            logger.info(f"Logs not found: {self.path}. Initializing...")
            self._flush(self.path)
        else:
            with tempfile.TemporaryDirectory() as temp_dir:
                self._flush(temp_dir)
                self._assert_equal_directory_contents(temp_dir, self.path)


lazy_tester = LazyTester(verbose=False)


def test_lazy_tester():
    lazy_tester = LazyTester()
    lazy_tester.configure(app_path=".")
    lazy_tester.configure_function_path()
    lazy_tester.log("This is a test", var="<var>")
    lazy_tester.assert_logs()


if __name__ == "__main__":
    test_lazy_tester()
