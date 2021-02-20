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


if __name__ == "__main__":
    print(to_lower_name("camelCase"))  # camel_case
    print(to_lower_name("UpperCase"))  # upper_case
