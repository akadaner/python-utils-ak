from utils_ak.networking.proxy.availability_proxy_guard import AvailabilityProxyGuard
from utils_ak.networking.proxy.limit_usage_manager import LimitUsageManager


class TimeLimitProxyGuard(AvailabilityProxyGuard):
    def __init__(self, proxy_config, *args, proxy_limits=None, **kwargs):
        super().__init__(proxy_config, *args, **kwargs)

        if proxy_limits is None:
            raise Exception("This proxy guard can`t work without limit")

        proxies = proxy_config.keys()
        self.limit_usage_manager = LimitUsageManager(proxy_limits, proxies)

    @property
    def available_counter(self):
        counter = super().available_counter

        limit_exceeded_proxies = []
        for proxy in counter.keys():
            if not self.limit_usage_manager.is_active(proxy):
                limit_exceeded_proxies.append(proxy)

        for proxy in limit_exceeded_proxies:
            del counter[proxy]

        return counter

    def on_acquire_proxy_callback(self, proxy):
        self.limit_usage_manager.add_usage(proxy)


if __name__ == "__main__":
    import time

    config = {"http://localhos:80": 10, "http://192.168.0.1:80": 10}
    # seconds: usage_limit
    limits = {1: 1, 4: 2}
    proxy_guard = TimeLimitProxyGuard(
        config, check_proxy_availability=False, proxy_limits=limits
    )

    time_started = time.time()

    assert len(proxy_guard.available_counter.items()) == 2
    proxy_guard.acquire_proxy()
    proxy_guard.acquire_proxy()
    assert len(proxy_guard.available_counter.items()) == 0

    time.sleep(1)
    assert len(proxy_guard.available_counter.items()) == 2
    proxy_guard.acquire_proxy()
    assert len(proxy_guard.available_counter.items()) == 1

    time.sleep(1)
    assert len(proxy_guard.available_counter.items()) == 1
    proxy_guard.acquire_proxy()
    assert len(proxy_guard.available_counter.items()) == 0

    time.sleep(1)
    assert len(proxy_guard.available_counter.items()) == 0

    time.sleep(1)
    assert len(proxy_guard.available_counter.items()) == 2

    # this assert validate that we not sleep when use acquire_proxy
    assert time.time() - time_started < 4.1

    print("Test TimeLimitProxyGuard is OK")
