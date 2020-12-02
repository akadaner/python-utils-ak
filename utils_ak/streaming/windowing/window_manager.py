import logging


class WindowManager:
    def add(self, key, value):
        raise NotImplemented()


class BasicWindowManager:
    def __init__(self, window_generator):
        self.windows = {}  # {key: window}
        self.window_generator = window_generator

    def add(self, key, value):
        if key not in self.windows:
            self.windows[key] = self.window_generator(key)

        window = self.windows[key]
        if window.state == 'closed':
            logging.warning(f'Trying to add value to a closed window {key}')

        window.add(value)

        if window.is_closeable():
            return window.close()

    def close_if_possible(self):
        res = {k: w for k, w in self.windows.items() if w.state == 'open' and w.is_closeable()}
        return {k: w.close() for k, w in res.items()}


if __name__ == '__main__':
    from utils_ak.streaming import ProcessingSessionWindow

    wm = BasicWindowManager(lambda key: ProcessingSessionWindow(gap=2))

    wm.add('key', 'asdf')

    import time
    print(wm.close_if_possible())
    time.sleep(1)
    print(wm.close_if_possible())
    time.sleep(1)
    print(wm.close_if_possible())
