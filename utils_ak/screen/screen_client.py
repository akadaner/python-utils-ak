import pandas as pd
import fnmatch
import os

from utils_ak.os import *

from loguru import logger


class ScreenClient:
    def start(self, name, cmd):
        makedirs("screen_logs/")
        logger.info(f"Starting screen {name} with command {cmd}")
        execute(
            f'screen -dm -S {name} -L -Logfile screen_logs/{name}.log bash -c "{cmd}"',
            is_async=True,
        )

    def start_python(self, name, path, **kwargs):
        if not os.path.exists(path):
            raise Exception(f"File does not exist {path}")

        cmd = f"python {path}"
        for k, v in kwargs.items():
            cmd += f"--{k} {v} "

        self.start(name, cmd)

    def _filter(self, df, filter):
        if isinstance(filter, str):
            _filter = str(filter)
            filter = lambda s: fnmatch.fnmatch(s, _filter)
        if filter:
            df = df[df["name"].apply(filter)]
        return df

    def list(self, filter=None, order_by="time"):
        output = execute("screen -ls", is_async=False)
        lines = output.split("\n")[1:-2]

        values = []
        for line in lines:
            value = line.split("\t")[1:]
            value = value[0].split(".", 1) + value[1:]
            if len(value) == 3:
                # no time provided
                value = value[:2] + [""] + value[2:3]
            values.append(value)

        df = pd.DataFrame(values, columns=["id", "name", "time", "status"])
        df["status"] = df["status"].apply(lambda s: s.replace("(", "").replace(")", ""))
        df["time"] = df["time"].apply(lambda s: s.replace("(", "").replace(")", ""))
        df["time"] = pd.to_datetime(df["time"])
        df = df[["time", "name", "id", "status"]]
        df = df.sort_values(by=order_by)
        df = self._filter(df, filter)
        df = df.reset_index(drop=True)
        return df

    def kill_one(self, name):
        logger.info(f"Killing screen {name}")
        return execute(f"screen -X -S {name} quit", is_async=False)

    def kill(self, filter):
        df = self.list()

        if len(df) == 0:
            return

        df["full_name"] = df["id"].astype(str) + "." + df["name"]
        df = self._filter(df, filter)

        for i, row in df.iterrows():
            self.kill_one(row["full_name"])

    def restart(self, name, cmd):
        self.kill(name)
        self.start(name, cmd)


def test():
    import time

    cli = ScreenClient()
    cli.start("test", "sleep 10")
    print(cli.list())
    print(cli.list("t*"))
    print(cli.list("b*"))  # empty
    time.sleep(1)
    cli.kill("t*")
    print(cli.list())

    print(cli.start_python("hello-world", "tests/hello_world.py"))
    print(cli.list("hello-world"))
    time.sleep(1)
    cli.kill("hello-world")
    print(cli.list("hello-world"))


if __name__ == "__main__":
    test()
