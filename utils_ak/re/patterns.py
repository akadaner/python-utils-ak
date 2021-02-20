def float_pattern():
    return r"([+-]?[0-9]*[.]?[0-9]+)"


def spaces_on_edge(where="beg"):
    if where == "beg":
        return r"^ *"
    elif where == "end":
        return r" *$"
    else:
        raise Exception("Unknown where")
