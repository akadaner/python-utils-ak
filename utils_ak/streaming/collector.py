from utils_ak.builtin import update_dic


class Collector:
    def __init__(self, fields, sources=None):
        # {key: {field: {symbol: value}}}
        self.unfilled_bars = {}

        # {key: <list of fields>}}
        self.filled_fields = {}

        # {key: bool}
        self.sent = {}

        self.fields = fields
        self.sources = sources or []

    def is_sent(self, key):
        return key in self.sent

    def add(self, key, fields_dic, source=None):
        """
        :param source: str
        :param key: any key value
        :param fields_dic: {field: {symbols: value}}
        :return:
        """
        if source and source not in self.sources:
            return

        if self.is_sent(key):
            raise Exception(f'Already sent key {key}')

        if key not in self.unfilled_bars:
            # new key
            self.unfilled_bars[key] = fields_dic
            self.filled_fields[key] = list(fields_dic.keys())
        else:
            # old key
            self.unfilled_bars[key] = update_dic(self.unfilled_bars[key], fields_dic)
            self.filled_fields[key] += list(fields_dic.keys())

        # remove fields, that are not expected
        self.filled_fields[key] = list(set(self.filled_fields[key]).intersection(set(self.fields)))

        return self.is_filled(key)

    def is_filled(self, key):
        return set(self.filled_fields.get(key, [])) == set(self.fields)

    def is_ready_to_be_sent(self, key):
        return self.is_filled(key) and not self.is_sent(key)

    def send(self, key):
        value = self.unfilled_bars.pop(key)
        self.filled_fields.pop(key)
        self.sent[key] = True
        return value


if __name__ == '__main__':
    collector = Collector(fields=['a', 'b'])

    print(collector.add('key', {'a': 1}))
    print(collector.is_filled('key'))
    print(collector.add('key', {'b': 1}))
    print(collector.send('key'))

