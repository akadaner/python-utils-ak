def to_lower_name(upper_case_name):
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
    values = lower_case_name.split("_")
    values = [v.title() for v in values]
    if camel:
        first = values[0]
        values[0] = first[0].lower() + first[1:]
    return "".join(values)


if __name__ == "__main__":
    print(to_lower_name("camelCase"))  # camel_case
    print(to_lower_name("UpperCase"))  # upper_case
    print(to_upper_case("upper_case"))  # UpperCase
    print(to_upper_case("upper_case", camel=True))  # upperCase
