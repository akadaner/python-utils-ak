def trim(s, beg=None, end=None):
    if beg is not None and s.startswith(beg):
        s = s[len(beg):]
    if end is not None and s.endswith(end):
        s = s[:-len(end)]
    return s


def get_source_code(method):
    import inspect
    lines = inspect.getsourcelines(method)[0]
    def_line = lines[0]
    offset_len = def_line.index(def_line.strip())
    offset = def_line[:offset_len]
    lines = [trim(l, beg=offset) for l in lines]
    return ''.join(lines)


