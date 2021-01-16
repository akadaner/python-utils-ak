

class SimpleIterator:
    def __init__(self, lst, start_from=0):
        self.lst = lst
        self.current_index = start_from

    def current(self):
        return self.lst[self.current_index]

    def __len__(self):
        return len(self.lst)

    def forward(self, step=1, return_last_if_out=False, update_index=True):
        if self.current_index >= len(self.lst) - step:
            if return_last_if_out:
                res = self.lst[-1]
            else:
                res = None
        else:
            res = self.lst[self.current_index + step]

        if update_index:
            self.current_index = min(self.current_index + step, len(self.lst) - 1)
        return res

    def next(self, return_last_if_out=False, update_index=True):
        return self.forward(1, return_last_if_out, update_index)

    def backward(self, step=1, return_first_if_out=False, update_index=True):
        if self.current_index <= step - 1:
            if return_first_if_out:
                res = self.lst[0]
            else:
                res = None
        else:
            res = self.lst[self.current_index - 1]

        if update_index:
            self.current_index = max(self.current_index - step, 0)
        return res

    def prev(self, return_first_if_out=False, update_index=True):
        return self.backward(1, return_first_if_out, update_index)

    def iter(self, step=1, direction='up'):
        yield self.current()
        while True:
            if direction == 'up':
                if self.current_index == len(self.lst) - 1:
                    break
                yield self.forward(step)
            elif direction == 'down':
                if self.current_index == 0:
                    break
                yield self.backward(step)

    def iter_sequences(self, n=2):
        for i in range(len(self) - n + 1):
            yield self.lst[i: i + n]

    def reset(self):
        self.current_index = 0


def test_simple_bounded_iterator():
    lst = [1, 2, 3, 4]
    it = SimpleIterator(lst)

    for v in it.iter('up'):
        print(v)
    print()
    for v in it.iter('down'):
        print(v)
    print()
    for i in range(5):
        print(it.next(return_last_if_out=False))
    print()
    for i in range(5):
        print(it.prev(return_first_if_out=True))

    for seq in it.iter_sequences(2):
        print(seq)

    it.reset()

    # for v in it.forward(2):



if __name__ == '__main__':
    test_simple_bounded_iterator()
