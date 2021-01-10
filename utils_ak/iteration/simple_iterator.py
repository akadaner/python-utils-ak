

class SimpleBoundedIterator:
    def __init__(self, lst, start_from=0):
        self.lst = lst
        self.cur_ind = start_from

    def cur(self):
        return self.lst[self.cur_ind]

    def next(self):
        if self.cur_ind == len(self.lst) - 1:
            return
        self.cur_ind += 1
        return self.cur()

    def prev(self):
        if self.cur_ind == 0:
            return
        self.cur_ind -= 1
        return self.cur()

    def iter(self, direction='up'):
        yield self.cur()
        while True:
            if direction == 'up':
                if self.cur_ind == len(self.lst) - 1:
                    break
                yield self.next()
            elif direction == 'down':
                if self.cur_ind == 0:
                    break
                yield self.prev()


def test_simple_bounded_iterator():
    lst = [1, 2, 3, 4]
    it = SimpleBoundedIterator(lst)

    for v in it.iter('up'):
        print(v)

    for v in it.iter('down'):
        print(v)

    for i in range(5):
        print(it.next())

    for i in range(5):
        print(it.prev())


if __name__ == '__main__':
    test_simple_bounded_iterator()