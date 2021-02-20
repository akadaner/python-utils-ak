import fnmatch
import shutil
from contextlib import contextmanager
from _sha512 import sha512
from uuid import uuid4
import os
import tempfile as tmp
import subprocess
import os
import platform


from utils_ak.str import cast_bytes


def make_directories(path):
    dirname = os.path.dirname(path)
    if not dirname:
        return
    if not os.path.exists(dirname):
        os.makedirs(dirname)


makedirs = make_directories


def remove_path(path):
    if not os.path.exists(path):
        return
    elif os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    else:
        raise Exception("Path {} is not a file or a dir".format(path))


def rename_path(path1, path2):
    os.rename(path1, path2)


def copy_path(path_from, path_to):
    shutil.copy(path_from, path_to)


def touch_file(fn):
    with open(fn, "w") as f:
        pass


def remove_empty_directories(path, remove_root=True):
    """Function to remove empty folders"""
    if not os.path.isdir(path):
        return

    # remove empty subfolders
    fns = os.listdir(path)

    for fn in fns:
        fullpath = os.path.join(path, fn)
        if os.path.isdir(fullpath):
            remove_empty_directories(fullpath)

    # if folder empty, delete it
    fns = os.listdir(path)

    if not fns and remove_root:
        os.rmdir(path)


def list_files(path, pattern=None, recursive=True):
    if not recursive:
        fns = os.listdir(path)
    else:
        # glob.glob('**/*') is slower 2.5 times than simple os.walk. It also returns directories
        fns = []
        for root, dirs, files in os.walk(path):
            # todo: make os.path.join faster?
            fns += [os.path.join(root, fn) for fn in files]
    if pattern:
        fns = [fn for fn in fns if fnmatch.fnmatch(fn, pattern)]
    return fns


def gen_fn_hash(s):
    return sha512(s).hexdigest()


def gen_tmp_fn(s=None, extension=None):
    if not s:
        s = uuid4().hex
    s = cast_bytes(s, encoding="utf-8")
    res = os.path.join("/tmp/", gen_fn_hash(s))
    if extension:
        res = res + extension
    return res


@contextmanager
def tempfile(*args, **kwargs):
    """Context for temporary file.

    Will find a free temporary filename upon entering
    and will try to delete the file on leaving, even in case of an exception.

    Parameters
    ----------
    suffix : string
        optional file suffix
    dir : string
        optional directory to save temporary file in
    """

    tf = tmp.NamedTemporaryFile(delete=False, *args, **kwargs)
    tf.file.close()
    try:
        yield tf.name
    finally:
        try:
            os.remove(tf.name)
        except OSError as e:
            if e.errno == 2:
                pass
            else:
                raise


@contextmanager
def open_atomic(filepath, *args, **kwargs):
    """Open temporary file object that atomically moves to destination upon
    exiting.

    Allows reading and writing to and from the same filename.

    The file will not be moved to destination in case of an exception.

    Parameters
    ----------
    filepath : string
        the file path to be opened
    fsync : bool
        whether to force write the file to disk
    *args : mixed
        Any valid arguments for :code:`open`
    **kwargs : mixed
        Any valid keyword arguments for :code:`open`
    """
    fsync = kwargs.pop("fsync", False)

    with tempfile(dir=os.path.dirname(os.path.abspath(filepath))) as tmppath:
        with open(tmppath, *args, **kwargs) as file:
            try:
                yield file
            finally:
                if fsync:
                    file.flush()
                    os.fsync(file.fileno())
        os.rename(tmppath, filepath)


def getenv_boolean(var_name, default_value=False):
    result = default_value
    env_value = os.getenv(var_name)
    if env_value is not None:
        result = env_value.upper() in ("TRUE", "1")
    return result


def open_file_in_os(fn):
    fn = os.path.abspath(fn)
    if platform.system() == "Darwin":  # macOS
        subprocess.call(("open", fn))
    elif platform.system() == "Windows":  # Windows
        os.startfile(fn)
    else:  # linux variants
        subprocess.call(("xdg-open", fn))


if __name__ == "__main__":
    print(gen_tmp_fn(extension=".operation.canary"))
