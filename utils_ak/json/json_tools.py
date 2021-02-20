import json
from pathlib import Path
import os


def write_json(path, json_string):
    dir_path = Path(os.path.dirname(path))
    dir_path.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as outfile:
        json.dump(json_string, outfile)


def read_json(path):
    with open(path, "r") as infile:
        return json.load(infile)
