import inspect

from inspect import FrameInfo
from pathlib import Path


def get_frame_path(
    frame_num: int,  # 0 - current frame, 1 - parent frame, ...
) -> Path:
    # - Get the current frame

    current_frame = inspect.currentframe()

    # - Get the frame

    caller_frame: FrameInfo = inspect.getouterframes(current_frame)[
        frame_num + 1
    ]  # 0: tested, 1: get_frame_path, 2: caller, ...

    # - Extract the file name from the frame

    return Path(caller_frame.filename)


def get_parent_frame_path():
    return get_frame_path(frame_num=1)
