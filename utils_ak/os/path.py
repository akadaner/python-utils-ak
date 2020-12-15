import os


# todo: search for existing solutions
class Path:
    def __init__(self, path):
        self.path = path
        self.base, self.ext = os.path.splitext(path)
        self.dirname = os.path.dirname(path)
        self.basename = os.path.basename(path)
