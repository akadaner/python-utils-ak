import multiprocessing
import random


class ProxyGuard(object):
    """
    I'm a thread-safe guard.
    I have proxies, proxies have a number of threads that can use them at once.
    Someone can ask me to give him a proxy. I give it. If no proxies are available, I return None
    """

    def __init__(self, proxy_config):
        # {proxy: max_usages_at_once}
        self.counter = proxy_config
        self.lock = multiprocessing.Lock()

    def acquire_proxy(self):
        self.lock.acquire(True)

        selected_proxy = None
        items = list(self.counter.items())
        random.shuffle(items)
        for key, val in items:
            if val > 0:
                selected_proxy = key
                self.counter[key] -= 1
                break

        self.lock.release()

        return selected_proxy

    def release_proxy(self, proxy):
        self.lock.acquire(True)
        self.counter[proxy] += 1
        self.lock.release()
