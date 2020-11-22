from collections import OrderedDict


class OrderedDictWithDuplicates(OrderedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._values = {}  # {key: values}

    def __delitem__(self, key):
        if key in self._values:
            # remove last element in values
            self._values[key] = self._values[key][:-1]

        if self._values[key]:
            super().__setitem__(key, self._values[key][-1])
        else:
            # remove last element from OrderedDict
            super().__delitem__(key)

        # clear key from _values if necessary
        if not self._values[key]:
            self._values.pop(key)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._values.setdefault(key, []).append(value)

    def get_with_duplicates(self, key, default=None):
        return self._values.get(key, default)

    def __str__(self):
        return 'OrderedDictWithDuplicates([{}])'.format(', '.join([f'{key}: {values}' for key, values in self._values.items()]))

    def __repr__(self):
        return str(self)

    def pop_with_duplicates(self, key):
        if key in self._values:
            res = self._values[key]

            # clean dictionaries
            self._values[key] = []
            super().__delitem__(key)
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
    print(d.get_with_duplicates('2'))
    print(d.pop_with_duplicates('2'))
    print(d)