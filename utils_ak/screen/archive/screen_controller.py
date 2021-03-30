import fnmatch


class ScreenController:
    def run(self, filter=None):
        config = self.config
        df = self.list()
        if filter:
            config = {k: v for k, v in config.items() if fnmatch.fnmatch(k, filter)}
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
        df = self.list()
        if pattern:
            config = {
                k: v for k, v in self.config.items() if fnmatch.fnmatch(k, pattern)
            }
        for screen_name, path in config.items():
            if screen_name not in df["name"].values:
                df.loc[len(df)] = [None, screen_name, None, "Not running"]
        return df.sort_values(by="name").reset_index(drop=True)
