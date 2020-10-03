class ProxyBag(object):
    """ I'm a bag of proxies. I store them, I give them :) """

    def __init__(self, proxies):
        super().__init__()

        self.proxies = proxies

    def all(self):
        return self.proxies

    def without(self, forsaken_proxies):
        return filter(lambda proxy: proxy not in forsaken_proxies, self.proxies)

    def add(self, new_proxies: list):
        self.proxies += new_proxies
