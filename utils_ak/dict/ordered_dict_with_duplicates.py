from collections import OrderedDict


class OrderedDictWithDuplicates:
    def __init__(self):
        self._values = {}  # {key: values}

    def __setitem__(self, key, value):
        self._values.setdefault(key, []).append(value)

    def __delitem__(self, key):
        if key in self._values:
            # remove last element in values
            self._values[key] = self._values[key][:-1]

            # clear key from _values if necessary
            if not self._values[key]:
                self._values.pop(key)

    def __getitem__(self, item):
        return self._values.get(item)

    def __str__(self):
        return 'OrderedDictWithDuplicates([{}])'.format(', '.join([f'{key}: {values}' for key, values in self._values.items()]))

    def __repr__(self):
        return str(self)

    def get(self, key, default=None):
        return self._values.get(key, default)

    def pop(self, key):
        if key in self._values:
            res = self._values[key]

            # clean dictionaries
            self._values[key] = []
            return res

        raise KeyError(key)


if __name__ == '__main__':
    d = OrderedDictWithDuplicates()
    d['1'] = 1
    d['2'] = 1
    print(d)

    d['2'] = 2
    print(d)

    del d['2']
    print(d)

    del d['2']
    print(d)

    d = OrderedDictWithDuplicates()
    d['1'] = 1
    d['2'] = 1
    d['2'] = 2
    print(d['2'])
    print(d.get('2'))
    print(d)