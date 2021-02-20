import pandas as pd
import fnmatch
import os

from utils_ak.os import execute


class ScriptManager(object):
    def __init__(self, config, prefix="_"):
        self.prefix = prefix

        self.config = {self.cast_name(name): path for name, path in config.items()}

    def start(self, name, cmd):
        execute('screen -S {} -dm bash -c " {}"'.format(name, cmd), is_async=True)

    def ls(self, by="time"):
        output = execute("screen -ls", is_async=False)
        lines = output.split("\n")[1:-2]
        values = []
        for line in lines:
            value = line.split("\t")[1:]
            value = value[0].split(".", 1) + value[1:]
            values.append(value)
        df = pd.DataFrame(values, columns=["id", "name", "time", "status"])
        df["status"] = df["status"].apply(lambda s: s.replace("(", "").replace(")", ""))
        df["time"] = df["time"].apply(lambda s: s.replace("(", "").replace(")", ""))
        df["time"] = pd.to_datetime(df["time"])
        df = df[["time", "name", "id", "status"]]
        df = df.sort_values(by=by)
        df = df[df["name"].str.startswith(self.prefix)]
        df = df.reset_index(drop=True)
        return df

    def kill(self, pattern):
        df = self.ls()
        if len(df) == 0:
            return
        df["full_name"] = df["id"].astype(str) + "." + df["name"]
        df = df[df["name"].apply(lambda name: fnmatch.fnmatch(name, pattern))]
        df = df[df["name"] != "jupyter"]
        for i, row in df.iterrows():
            print("Killing: {}".format(row["full_name"]))
            self.kill_one(row["full_name"])

    def restart(self, pattern):
        self.kill(pattern)
        self.run(pattern)

    def kill_one(self, name):
        if name == "jupyter":
            raise Exception("jupyter cannot be killed for safety measures")
        return execute("screen -S " + name + " -X quit", is_async=False)

    def run_python(self, name, path):
        if not os.path.exists(path):
            raise Exception("File does not exist {}".format(path))
        self.start(name, "python {}".format(path))

    def run(self, pattern=None):
        config = self.config
        df = self.ls()
        if pattern:
            config = {k: v for k, v in config.items() if fnmatch.fnmatch(k, pattern)}
        for screen_name, path in config.items():
            if screen_name in df["name"].values:
                print(f"Already running: {screen_name}")
            else:
                print(f"Running: {screen_name}")
                self.run_python(screen_name, path)

    def cast_name(self, name):
        return f"{self.prefix}{name}"

    def state(self, pattern=None):
        config = self.config
        df = self.ls()
        if pattern:
            config = {
                k: v for k, v in self.config.items() if fnmatch.fnmatch(k, pattern)
            }
        for screen_name, path in config.items():
            if screen_name not in df["name"].values:
                df.loc[len(df)] = [None, screen_name, None, "Not running"]
        return df.sort_values(by="name").reset_index(drop=True)
