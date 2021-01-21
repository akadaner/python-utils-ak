import logging
import os
import sys
import time
import traceback
import asyncio

from utils_ak.callback_timer import CallbackTimer, ScheduleTimer, CallbackTimers
from utils_ak.architecture.func import PrefixHandler
from utils_ak.str import cast_unicode
from utils_ak.serialization import JsonSerializer
from utils_ak.message_queue import cast_message_broker

logger = logging.getLogger(__name__)


TIME_EPS = 0.001


class SimpleMicroservice(object):
    """ Microservice base class with timers and subscriber. Works on asyncio. """

    def __init__(self, id, message_broker, logger=None, serializer=None):
        self.id = id

        # aio
        self.tasks = []
        # sync
        self.timers = []

        # {collection: callback}
        self.callbacks = {}

        self.broker = cast_message_broker(message_broker)

        # {broker: f'{collection}::{topic}'}
        self.subscribed_to = {}

        self.logger = logger or logging.getLogger('-'.join([os.path.basename(sys.argv[0])]))

        self.default_exception_timeout = 10.
        self.max_exception_timeout = 3600
        self.fail_count = 0

        self.is_active = True

        self.serializer = serializer or JsonSerializer()

    def stop(self):
        self.is_active = False

    def _args_formatter(self, topic, msg):
        return (topic, self.serializer.decode(msg)), {}

    def add_timer(self, *args, **kwargs):
        """
        :param interval: timer will be run every interval
        :param callback: func
        """
        self.timers.append(CallbackTimer(*args, **kwargs))

    def add_schedule(self, *args, **kwargs):
        """
        :param pattern: cron-like pattern with seconds:
              sec min hour monthday month weekday
              *    5    *     *       *     *     -> run every 5th minute of every hour
        :param callback:
        :param init_run: bool
        """
        self.timers.append(ScheduleTimer(*args, **kwargs))

    def subscribe(self, collection, topic):
        self.broker.subscribe(collection, topic)

    def add_callback(self, collection, topic, callback=None, formatter='default',
                     filter=None, topic_formatter=cast_unicode):
        self.broker.subscribe(collection, topic)
        self._add_callback(collection, topic, callback, formatter, filter, topic_formatter)

    def _add_callback(self, collection, topic, callback=None,
                      formatter='default', filter=None, topic_formatter=cast_unicode):
        assert type(topic) == str, 'Topic must be str'

        if formatter == 'default':
            formatter = self._args_formatter

        handler = self.callbacks.setdefault(collection, PrefixHandler())
        handler.set_topic_formatter(topic_formatter)
        handler.add(topic, callback=callback, filter=filter, formatter=formatter)
        return handler

    def publish(self, collection, topic, msg):
        self.broker.publish(collection, topic, msg)

    def publish_json(self, collection, topic, msg):
        self.publish(collection, topic, self.serializer.encode(msg))

    def wrap_coroutine_timer(self, timer):
        async def f():
            while True:
                try:
                    has_ran = await timer.aiocheck()

                    if has_ran and self.fail_count != 0:
                        self.logger.info('Success. Resetting the failure counter')
                        self.fail_count = 0

                except Exception as e:
                    self.on_exception(e, 'Exception occurred at the timer callback')

                if not self.is_active:
                    return
                await asyncio.sleep(max(timer.next_call - time.time() + TIME_EPS, 0))

        return f()

    def wrap_coroutine(self, timeout=0.01):
        async_supported = self.broker.async_supported

        async def f():
            while True:
                try:
                    if async_supported:
                        received = await self.broker.aiopoll()
                    else:
                        received = self.broker.poll(timeout)
                    if not received:
                        await asyncio.sleep(0)
                        continue

                    collection, topic, msg = received
                    try:
                        self.logger.debug(f'Received new message', custom={'topic': topic, 'msg': msg})
                        await self.callbacks[collection].aiocall(topic, msg)

                        if self.fail_count != 0:
                            self.logger.info('Success. Resetting the failure counter')
                            self.fail_count = 0

                    except Exception as e:
                        self.on_exception(e, f"Exception occurred at the callback")
                except Exception as e:
                    self.on_exception(e, f'Failed to receive the message')

                if not self.is_active:
                    return

                await asyncio.sleep(0)

        return f()

    def _aiorun(self):
        self.loop = asyncio.get_event_loop()
        # self.loop.set_debug(True)
        self.logger.info(f'Microservice started: {self.id}')

        for timer in self.timers:
            self.tasks.append(self.wrap_coroutine_timer(timer))

        self.tasks.append(self.wrap_coroutine())

        self.tasks = [asyncio.ensure_future(task) for task in self.tasks]
        self.loop.run_until_complete(asyncio.wait(self.tasks))

    def _run(self, timeout=0.01):
        self.logger.info('Microservice started')

        self.callback_timers = CallbackTimers()
        for timer in self.timers:
            self.callback_timers.add_timer(timer)

        while True:
            success = False
            try:
                if len(self.timers) > 0:
                    if time.time() > self.callback_timers.next_call:
                        # some timer is ready
                        for timer in self.callback_timers.timers:
                            try:
                                timer.run_if_possible()
                            except Exception as e:
                                self.on_exception(e, 'Exception occurred at the timer callback')
                            else:
                                success = True

                try:
                    received = self.broker.poll(timeout)
                    if not received:
                        continue
                    collection, topic, msg = received
                    try:
                        self.logger.debug(f'Received new message', custom={'topic': topic, 'msg': msg})
                        self.callbacks[collection].call(topic, msg)
                    except Exception as e:
                        self.on_exception(e, f"Exception occurred")
                    else:
                        success = True

                except Exception as e:
                    self.on_exception(e, 'Failed to receive the message')

                if not self.is_active:
                    self.logger.info('Microservice not active. Stopping')
                    break

            except Exception as e:
                self.on_exception(e, 'Global exception occurred')

            if success and self.fail_count != 0:
                self.logger.info('Success. Resetting the failure counter')
                self.fail_count = 0

    def run(self, asyncio=True):
        if asyncio:
            return self._aiorun()
        else:
            return self._run()

    def on_exception(self, e, msg):
        self.logger.error((e, msg))
        # todo: log traceback properly
        traceback.print_exc()
        to_sleep = min(self.default_exception_timeout * 2 ** (self.fail_count - 1), self.max_exception_timeout)
        time.sleep(to_sleep)
        self.fail_count += 1
