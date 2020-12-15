import os
import glob


class SplitFile:
    def __init__(self, fn):
        self.fn = fn

    def list(self):
        return glob.glob(os.path.splitext(self.fn)[0] + '*')

    def get_indexes(self):
        cur_indexes = []
        for fn in self.list():
            if fn == self.fn:
                cur_indexes.append(0)
            else:
                cur_indexes.append(int(os.path.splitext(fn)[0].rsplit('_', 1)[-1]))
        return cur_indexes

    def get_current(self, max_size=None):
        cur_indexes = self.get_indexes()

        if not cur_indexes:
            return

        suffix = '' if max(cur_indexes) == 0 else '_{}'.format(max(cur_indexes))
        cur_fn = os.path.splitext(self.fn)[0] + suffix + os.path.splitext(self.fn)[-1]

        if max_size and os.path.getsize(cur_fn) > max_size:
            return self.get_new()
        else:
            return cur_fn

    def get_new(self):
        cur_indexes = self.get_indexes()
        if not cur_indexes:
            return self.fn

        suffix = '_{}'.format(max(cur_indexes) + 1)
        return os.path.splitext(self.fn)[0] + suffix + os.path.splitext(self.fn)[-1]


if __name__ == '__main__':
    sf = SplitFile('test.csv')
    print(sf.list(), sf.get_indexes(), sf.get_current(), sf.get_new())
    with open(sf.get_new(), 'w') as f:
        pass
    print(sf.list(), sf.get_indexes(), sf.get_current(), sf.get_new())
    with open(sf.get_new(), 'w') as f:
        pass
    print(sf.list(), sf.get_indexes(), sf.get_current(), sf.get_new())

    for fn in sf.list():
        print('Removing', fn)
        os.remove(fn)
