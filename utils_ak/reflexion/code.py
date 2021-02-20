from utils_ak.str import trim


def get_source_code(method):
    import inspect

    lines = inspect.getsourcelines(method)[0]
    def_line = lines[0]
    offset_len = def_line.index(def_line.strip())
    offset = def_line[:offset_len]
    lines = [trim(l, beg=offset) for l in lines]
    return "".join(lines)
