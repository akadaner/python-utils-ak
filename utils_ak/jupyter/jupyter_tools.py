import urllib
import json
import os
import re

from utils_ak.os import remove_path, execute


def get_current_notebook_path():
    from notebook import notebookapp
    import ipykernel

    """Returns the absolute path of the Notebook or None if it cannot be determined
    NOTE: works only when the security is token-based or there is also no password
    """
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split("-", 1)[1].split(".")[0]

    for srv in notebookapp.list_running_servers():
        try:
            if (
                srv["token"] == "" and not srv["password"]
            ):  # No token and no password, ahem...
                req = urllib.request.urlopen(srv["url"] + "api/sessions")
            else:
                req = urllib.request.urlopen(
                    srv["url"] + "api/sessions?token=" + srv["token"]
                )
            sessions = json.load(req)
            for sess in sessions:
                if sess["kernel"]["id"] == kernel_id:
                    return os.path.join(srv["notebook_dir"], sess["notebook"]["path"])
        except:
            pass  # There may be stale entries in the runtime directory
    return None


def get_current_notebook_code():
    path = get_current_notebook_path()
    path = path.replace("\\", "/")
    s = fr'jupyter nbconvert --to script "{path}"'
    execute(s)
    with open(path.replace(".ipynb", ".py"), "r") as f:
        code = f.read()
    remove_path(path.replace(".ipynb", ".py"))
    return code


def parse_cells(notebook_code):
    cell_headers = re.findall(r"(In\[[\d\s]+\])", notebook_code)
    for cell_header in cell_headers:
        notebook_code = re.sub(
            cell_header.replace("[", "\[").replace("]", "\]"), "In[]", notebook_code, 1
        )

    res = ""
    for line in notebook_code.split("\n"):
        if "In[]" in line:
            res += ">split_cell"
        else:
            res += line
            res += "\n"
    cells = res.split(">split_cell")

    return cells[1:]


def is_running_in_ipython():
    try:
        cfg = get_ipython().config
        if cfg["IPKernelApp"]["parent_appname"] == "ipython-notebook":
            return True
        else:
            return False
    except NameError:
        return False


def is_running_in_jupyter():
    # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def safe_display(obj):
    if is_running_in_ipython():
        from IPython.display import display

        display(obj)
    else:
        print(obj)


def safe_markdown(obj):
    if is_running_in_ipython():
        from IPython.display import Markdown

        Markdown(obj)
    else:
        print(obj)
