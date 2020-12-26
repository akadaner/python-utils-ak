import re

def def_group(name, pat):
    return r'(?P<{}>{})'.format(name, pat)


def ref_group(name):
    return r'(?P={})'.format(name)


def replace_with_pattern(s, key, rep_name, rep_pat):
    if key not in s:
        return s
    s = s.replace(key, def_group(rep_name, rep_pat), 1)
    while key in s:
        s = s.replace(key, ref_group(rep_name))
    return s


def non_capturing(pat):
    return r'(?:{})'.format(pat)


def or_op(pat1, pat2, capturing=True):
    if capturing:
        return '({}|{})'.format(pat1, pat2)
    else:
        return non_capturing('{}|{}'.format(pat1, pat2))


def search_many(pattern, s):
    search = re.search(pattern, s)
    if not search:
        return
    return search.groups()


def search_one(pattern, s):
    res = search_many(pattern, s)
    if not res:
        return None
    return res[0]


def ex_replace_with_pattern():
    pat = replace_with_pattern('<pat> a <pat> b <pat> c', '<pat>', 'foo', '.+')
    print(pat)
    print(re.search(pat, 'aaa a aaa b aaa c').groupdict())
    print(re.search(pat, 'aaa a aaa b aaa c').groups())
    print(re.search(pat, 'asdf a asdf b asdf c').groupdict())
    print(re.search(pat, 'asdf1 a asdf2 b asdf3 c') is None)


if __name__ == '__main__':
    ex_replace_with_pattern()
