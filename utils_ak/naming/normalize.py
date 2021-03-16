def to_lower_case(upper_case_name):
    res = ""
    for l in upper_case_name:
        if not res:
            if l.isupper():
                res += l.lower()
            else:
                res += l
        else:
            if l.isupper():
                res += "_"
                res += l.lower()
            else:
                res += l
    return res


def to_upper_case(lower_case_name, camel=False):
    assert all(not c.isupper() for c in lower_case_name)
    values = lower_case_name.split("_")
    values = [v.title() for v in values]
    if camel:
        first = values[0]
        values[0] = first[0].lower() + first[1:]
    return "".join(values)


def test():
    assert to_lower_case("camelCase") == "camel_case"
    assert to_lower_case("UpperCase") == "upper_case"
    assert to_lower_case("upper_case") == "upper_case"
    assert to_upper_case("upper_case") == "UpperCase"
    assert to_upper_case("upper_case", camel=True) == "upperCase"

    try:
        to_upper_case("UpperCase") == "UpperCase"
    except AssertionError:
        print("Wrong input")


if __name__ == "__main__":
    test()
