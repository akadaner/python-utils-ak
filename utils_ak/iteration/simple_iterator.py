STUB = "__stub"


class SimpleIterator:
    def __init__(self, lst, start_from=0):
        self.lst = lst
        self.current_index = start_from

    def current(self):
        return self.lst[self.current_index]

    def __len__(self):
        return len(self.lst)

    def forward(
        self, step=1, return_last_if_out=False, update_index=True, out_value=None
    ):
        if self.current_index >= len(self.lst) - step:
            if return_last_if_out:
                res = self.lst[-1]
            else:
                res = out_value
        else:
            res = self.lst[self.current_index + step]

        if update_index:
            self.current_index = min(self.current_index + step, len(self.lst) - 1)
        return res

    def next(self, return_last_if_out=False, update_index=True):
        return self.forward(1, return_last_if_out, update_index)

    def backward(
        self, step=1, return_first_if_out=False, update_index=True, out_value=None
    ):
        if self.current_index <= step - 1:
            if return_first_if_out:
                res = self.lst[0]
            else:
                res = out_value
        else:
            res = self.lst[self.current_index - 1]

        if update_index:
            self.current_index = max(self.current_index - step, 0)
        return res

    def prev(self, return_first_if_out=False, update_index=True):
        return self.backward(1, return_first_if_out, update_index)

    def iter(self, direction="up", step=1, limit=None):
        yield self.current()
        counter = 1
        while True:
            if counter == limit:
                break

            if direction == "up":
                next = self.forward(step, out_value=STUB)
            elif direction == "down":
                next = self.backward(step, out_value=STUB)

            if next == STUB:
                break

            yield next
            counter += 1

    def iter_sequences(self, n=2, method="all"):
        assert method in ["all", "any"]

        if method == "all":
            lst = self.lst
        elif method == "any":
            # todo: optimize without creating a new list
            lst = [None] * (n - 1) + self.lst + [None] * (n - 1)

        for i in range(len(lst) - n + 1):
            yield lst[i : i + n]

    def reset(self):
        self.current_index = 0


def test_simple_bounded_iterator():
    lst = [1, 2, 3, 4]
    it = SimpleIterator(lst)

    print("Iter up")
    for v in it.iter("up"):
        print(v)
    print("Iter down")
    for v in it.iter("down"):
        print(v)
    print("return_last_if_out: false")
    for i in range(5):
        print(it.next(return_last_if_out=False))
    print("return_last_if_out: true")
    for i in range(5):
        print(it.prev(return_first_if_out=True))

    print("Sequences")
    print("all-2")
    for seq in it.iter_sequences(2):
        print(seq)

    print("any-2")
    for seq in it.iter_sequences(2, method="any"):
        print(seq)

    print("all-5")
    for seq in it.iter_sequences(5):
        print(seq)

    print("any-5")
    for seq in it.iter_sequences(5, method="any"):
        print(seq)

    it.reset()
    print("Step-2")
    for v in it.iter(step=2):
        print(v)
    it.reset()

    print("limit-2")
    for v in it.iter(limit=2):
        print(v)


if __name__ == "__main__":
    test_simple_bounded_iterator()
