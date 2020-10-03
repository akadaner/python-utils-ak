import time


class LimitUsageManager(object):
    def __init__(self, limits, items):
        self.limits = limits
        self.max_time_limit = max(self.limits.keys())
        self.usage_times = {item: [] for item in items}

    def is_active(self, item):
        if item not in self.usage_times.keys():
            raise Exception('Wrong item')

        for time_interval, limit in self.limits.items():
            usage_times = self.usage_times[item]

            if len(usage_times) < limit:
                continue

            last_limit_usage = usage_times[-limit]
            if time.time() - last_limit_usage < time_interval:
                return False

        return True

    def _clean_old_usages(self, item):
        usage_times = self.usage_times[item]
        old_usages_count = 0
        for usage_time in usage_times:
            if usage_time > time.time() - self.max_time_limit:
                break

            old_usages_count += 1

        self.usage_times[item] = usage_times[old_usages_count:]

    def add_usage(self, item):
        self.usage_times[item].append(time.time())
        self._clean_old_usages(item)


if __name__ == '__main__':
    limit_usage_manager = LimitUsageManager({2: 2, 3: 3}, ['first', 'second', 'third'])

    assert limit_usage_manager.is_active('first')
    assert limit_usage_manager.is_active('second')
    assert limit_usage_manager.is_active('third')

    limit_usage_manager.add_usage('first')
    limit_usage_manager.add_usage('first')
    limit_usage_manager.add_usage('first')
    limit_usage_manager.add_usage('second')
    limit_usage_manager.add_usage('second')
    limit_usage_manager.add_usage('third')

    assert not limit_usage_manager.is_active('first')
    assert not limit_usage_manager.is_active('second')
    assert limit_usage_manager.is_active('third')

    time.sleep(2)

    assert not limit_usage_manager.is_active('first')
    assert limit_usage_manager.is_active('second')
    assert limit_usage_manager.is_active('third')

    time.sleep(1)

    assert limit_usage_manager.is_active('first')
    assert limit_usage_manager.is_active('second')
    assert limit_usage_manager.is_active('third')

    print("Test LimitUsageManager OK")
