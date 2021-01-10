

class SimpleBoundedIterator:
    def __init__(self, lst, start_from=0):
        self.lst = lst
        self.current_index = start_from

    def current(self):
        return self.lst[self.current_index]

    def next(self, return_current_if_out=True, update_index=True):
        if self.current_index == len(self.lst) - 1:
            if return_current_if_out:
                return self.current()
            else:
                return
        res = self.lst[self.current_index + 1]
        if update_index:
            self.current_index += 1
        return res

    def prev(self, return_current_if_out=True, update_index=True):
        if self.current_index == 0:
            if return_current_if_out:
                return self.current()
            else:
                return
        res = self.lst[self.current_index - 1]
        if update_index:
            self.current_index -= 1
        return res

    def iter(self, direction='up'):
        yield self.current()
        while True:
            if direction == 'up':
                if self.current_index == len(self.lst) - 1:
                    break
                yield self.next()
            elif direction == 'down':
                if self.current_index == 0:
                    break
                yield self.prev()


def test_simple_bounded_iterator():
    lst = [1, 2, 3, 4]
    it = SimpleBoundedIterator(lst)

    for v in it.iter('up'):
        print(v)
    print()
    for v in it.iter('down'):
        print(v)
    print()
    for i in range(5):
        print(it.next(return_current_if_out=False))
    print()
    for i in range(5):
        print(it.prev(return_current_if_out=True))


if __name__ == '__main__':
    test_simple_bounded_iterator()