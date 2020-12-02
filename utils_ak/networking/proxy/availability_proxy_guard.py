import logging

import time
import multiprocessing
from requests_futures.sessions import FuturesSession
from utils_ak.networking.proxy import cast_requests_proxies
from utils_ak.callback_timer import CallbackTimer

logger = logging.getLogger(__name__)


class NoProxyAvailableException(Exception):
    pass


class AvailabilityProxyGuard:
    def __init__(self, proxy_config,
                 check_proxy_availability=True, check_proxy_url=None, check_proxy_params=None,
                 timeout=10, proxy_lives=3):
        # {proxy: max_usages_at_once}
        self.proxy_config = dict(proxy_config)

        # {proxy: available}
        self.counter = dict(proxy_config)
        self.max_proxy_lives = proxy_lives
        self.proxy_lives = {proxy: self.max_proxy_lives for proxy in proxy_config}

        # test proxies
        for proxy in self.proxy_config:
            if not self._is_proxy_valid(proxy):
                raise Exception(f'Proxy {proxy} is not valid')

        self.lock = multiprocessing.Lock()
        self.available_proxies = set(self.proxy_config.keys())

        self.timer = CallbackTimer(self.upd_availability, interval=300)
        self.check_proxy_availability = check_proxy_availability
        self.check_proxy_url = check_proxy_url
        self.check_proxy_params = check_proxy_params
        self.timeout = timeout

    def update_proxy_lives(self, proxy, success):
        if success:
            self.proxy_lives[proxy] = self.max_proxy_lives
        else:
            self.proxy_lives[proxy] -= 1

    @staticmethod
    def _is_proxy_valid(proxy):
        try:
            cast_requests_proxies(proxy)
        except Exception:
            return False
        else:
            return True

    @property
    def available_counter(self):
        return {proxy: available for proxy, available in self.counter.items() if proxy in self.available_proxies}

    @property
    def proxies(self):
        return list(self.counter.keys())

    @property
    def non_available_proxies(self):
        return set(self.proxies) - set(self.available_proxies)

    def on_acquire_proxy_callback(self, proxy):
        pass

    def acquire_proxy(self, timeout=None, sleep_timeout=0.01):
        started = time.time()

        while True:
            self.lock.acquire(True)

            self.timer.run_if_possible()

            if any(available > 0 for proxy, available in self.available_counter.items()):
                # found some available proxy. Note: lock is still acquired
                break

            # no available proxy
            self.lock.release()

            if timeout and time.time() - started > timeout:
                raise NoProxyAvailableException('No proxy available at the moment')

            time.sleep(sleep_timeout)

        proxy = max(self.available_counter.items(), key=lambda x: x[1])[0]
        logger.debug(f'Acquiring proxy {proxy}')
        self.counter[proxy] -= 1
        self.on_acquire_proxy_callback(proxy)
        self.lock.release()
        return proxy

    def release_proxy(self, proxy, success=True):
        logger.debug(f'Releasing proxy {proxy}')
        with self.lock:
            self.counter[proxy] += 1
            self.update_proxy_lives(proxy, success)

            if not success and self.proxy_lives[proxy] <= 0 and proxy in self.available_proxies:
                logger.debug(f'Proxy {proxy} is not available')
                self.available_proxies.remove(proxy)

    def upd_availability(self, failed_only=True):
        logger.debug(f'Start updating proxy availability. failed_only: {failed_only}')

        if not self.check_proxy_availability:
            return True

        if not self.check_proxy_url:
            raise Exception('Availability can be checked only with specified url')

        proxies = self.proxies if not failed_only else self.non_available_proxies

        if not proxies:
            logger.debug('No proxies to update')
            return

        session = FuturesSession(max_workers=300)

        responses = []
        for proxy in proxies:
            response = session.get(
                self.check_proxy_url,
                params=self.check_proxy_params,
                proxies=cast_requests_proxies(proxy),
                timeout=self.timeout)
            responses.append((proxy, response))

        for proxy, response in responses:
            success = self._is_availability_response_ok(response)
            self.update_proxy_lives(proxy, success)

            if success:
                # succeed
                if proxy not in self.available_proxies:
                    logger.debug(f'Proxy {proxy} is available')
                    self.available_proxies.add(proxy)
            else:
                # fail
                if proxy in self.available_proxies:
                    logger.debug(f'Proxy {proxy} is not available')
                    self.available_proxies.remove(proxy)

        logger.debug('Stop updating proxy availability')

    @staticmethod
    def _is_availability_response_ok(response):
        try:
            res = response.result()
            assert res.status_code == 200
        except Exception:
            return False
        else:
            return True


if __name__ == '__main__':
    from utils_ak.log import configure_stream_logging

    configure_stream_logging(level=logging.DEBUG)
    import random

    proxy_guard = AvailabilityProxyGuard({None: 5}, check_proxy_url='https://api.binance.com/api/v1/depth',
                                         check_proxy_params={'symbol': 'ETHBTC', 'limit': 100})
    proxy_guard.upd_availability(failed_only=False)

    for i in range(10):
        proxy_ = proxy_guard.acquire_proxy(timeout=None)

        print(proxy_, proxy_guard.counter)
        if random.randint(0, 2) == 0:
            proxy_guard.release_proxy(proxy_)
