import time
import asyncio
import inspect

from dateutil import rrule
from datetime import datetime, timedelta
from utils_ak.time import cast_sec, cast_timedelta


TIME_EPS = 0.001


class CallbackTimer:
    """ Run callback function every interval. """

    def __init__(self, callback, interval, timer_type="right", args=None, kwargs=None):
        """
        :param timer_type: str, "left" to start timing immediately, "right" to wait for callback() to finish
        """
        self.callback = callback
        self.interval = cast_sec(interval)
        self.last_call = 0
        self.timer_type = timer_type

        self.args = args or tuple()
        self.kwargs = kwargs or {}

        if timer_type not in ['right', 'left']:
            raise Exception('Only right/left timer is supported')

    @property
    def next_call(self):
        return self.last_call + self.interval

    def run_if_possible(self):
        if time.time() > self.next_call:
            if self.timer_type == "right":
                try:
                    self.run()
                except:
                    self.last_call = time.time()
                    raise
                else:
                    self.last_call = time.time()

            elif self.timer_type == "left":
                self.last_call = time.time()
                self.run()
            return True
        return False

    def run(self):
        return self.callback(*self.args, **self.kwargs)

    async def run_if_possible_aio(self):
        if time.time() > self.next_call:
            if self.timer_type == "right":
                try:
                    await self.run_aio()
                except:
                    self.last_call = time.time()
                    raise
                else:
                    self.last_call = time.time()
            elif self.timer_type == "left":
                self.last_call = time.time()
                await self.run_aio()
            return True
        return False

    async def run_aio(self):
        if inspect.iscoroutinefunction(self.callback):
            return await self.callback(*self.args, **self.kwargs)
        else:
            return self.callback(*self.args, **self.kwargs)


class ScheduleTimer:
    """ Run callback function at schedule. """
    def __init__(self, callback, pattern=None, freq=None, init_run=False, args=None, kwargs=None):
        """
        :param pattern: (seconds) (minutes) (hours) (day) (month) (day-of-week) in cron-like manner: * 5 * * * * ...
        """
        self.callback = callback
        self.next_call = 0
        self.init_run = init_run

        self.args = args or tuple()
        self.kwargs = kwargs or {}

        if freq:
            # cast pattern by frequency
            if cast_timedelta(freq) >= timedelta(days=1):
                # freq is supported only for hour and less frequency
                raise Exception(f'Too large frequency: {cast_sec(freq)}. Use frequency less than a day')
            freq_sec = round(cast_sec(freq))

            if freq_sec <= 0:
                raise Exception('Bad frequency: {}'.format(freq_sec))

            unit_secs = [1, 60, 3600, 3600 * 24]

            # find which unit is used
            unit_i = 0
            while True:
                if unit_secs[unit_i] <= freq_sec < unit_secs[unit_i + 1]:
                    break
                unit_i += 1

            if unit_secs[unit_i + 1] % freq_sec != 0:
                raise Exception('Bad frequency: {}'.format(freq_sec))

            freq_units = int(freq_sec / unit_secs[unit_i])
            freq_vals = [i * freq_units for i in range(unit_secs[unit_i + 1] // freq_sec)]
            freq_vals = map(str, freq_vals)

            pattern_vals = []
            for i in range(6):
                if i < unit_i:
                    pattern_vals.append('0')
                elif i == unit_i:
                    if freq_units != 1:
                        pattern_vals.append(','.join(freq_vals))
                    else:
                        pattern_vals.append('*')
                elif i > unit_i:
                    pattern_vals.append('*')
            self.pattern = ' '.join(pattern_vals)
        else:
            self.pattern = pattern
        units = sec, min, hou, day, mon, dow = [self._parse_time_unit(unit) for unit in self.pattern.split()]
        freqs = [rrule.SECONDLY, rrule.MINUTELY, rrule.HOURLY, rrule.DAILY, rrule.MONTHLY, rrule.DAILY]
        param_names = ['bysecond', 'byminute', 'byhour', 'bymonthday', 'bymonth', 'byweekday']

        try:
            freq = freqs[units.index(None)]
        except:
            # not stars at all
            freq = rrule.YEARLY

        self.rrule_params = dict(zip(param_names, units))
        self.rrule_params['freq'] = freq

        self._update_next_call()

    def _parse_time_unit(self, unit):
        if unit == '*':
            return None
        unit = unit.split(',')
        return [int(x) for x in unit]

    def _update_next_call(self):
        # set next_call to closest
        self.last_call_dt = datetime.fromtimestamp(self.next_call) if self.next_call else datetime.now()
        next_call_lst = list(rrule.rrule(dtstart=self.last_call_dt + timedelta(seconds=1),
                                         until=self.last_call_dt + timedelta(seconds=1) + timedelta(days=366),
                                         count=1,
                                         **self.rrule_params))

        if not next_call_lst:
            raise Exception('Bad scheduler input!')

        self.next_call = next_call_lst[0].timestamp()

    def run_if_possible(self):
        if self.init_run or time.time() > self.next_call:
            self.init_run = False
            self.run()
            self._update_next_call()
            return True
        return False

    async def run_if_possible_aio(self):
        if self.init_run or time.time() > self.next_call:
            self.init_run = False
            await self.run_aio()
            self._update_next_call()

    def run(self):
        return self.callback(*self.args, **self.kwargs)

    async def run_aio(self):
        if inspect.iscoroutinefunction(self.callback):
            return await self.callback(*self.args, **self.kwargs)
        else:
            return self.callback(*self.args, **self.kwargs)


class CallbackTimers(object):
    """ A builtin of callback timers. """

    def __init__(self):
        self.timers = []

    def add_timer(self, timer):
        self.timers.append(timer)

    def run_if_possible(self):
        if time.time() > self.next_call:
            for timer in self.timers:
                timer.run_if_possible()

    @property
    def next_call(self):
        return min([timer.next_call for timer in self.timers])

    def next_call_timeout(self):
        return max(self.next_call - time.time(), 0.)

    def __len__(self):
        return len(self.timers)

    async def run_if_possible_aio(self):
        if time.time() > self.next_call:
            for timer in self.timers:
                await timer.run_if_possible()


def test():
    def print_msg(msg=None):
        print('Callback:', datetime.now(), msg)

    print('Timer 1')
    timer1 = CallbackTimer(print_msg, 1)
    timer2 = CallbackTimer(print_msg, 1, args=('asdf',))
    for i in range(300):
        timer1.run_if_possible()
        timer2.run_if_possible()
        time.sleep(0.01)

    print('Timer 2')
    # run every second
    timer1 = ScheduleTimer(print_msg, '* * * * * *')
    timer2 = ScheduleTimer(print_msg, '* * * * * *', args=('asdf',))
    for i in range(300):
        timer1.run_if_possible()
        timer2.run_if_possible()
        time.sleep(0.01)

    print('Timer 3')
    # run every second
    timer = ScheduleTimer(print_msg, freq='2s', init_run=True)
    print(timer.pattern)
    N = 3 * 1000
    for i in range(N):
        timer.run_if_possible()
        time.sleep(1 / 10000)


def test_aio():
    import numpy as np
    async def print_msg(msg=None):
        timeout = np.random.uniform(0, 1)
        await asyncio.sleep(timeout)
        print('Callback:', datetime.now(), msg, timeout)

        if timeout > 0.5:
            raise Exception('Test')

    timer = CallbackTimer(print_msg, 1.0)

    async def wrapper():
        while True:
            try:
                await timer.coroutine()
            except Exception as e:
                print(e)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.ensure_future(wrapper()))
    loop.close()


if __name__ == '__main__':
    test()
    test_aio()
