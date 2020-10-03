import logging
import os
import sys
import time
import traceback
import asyncio

from utils_ak.callback_timer import CallbackTimer, ScheduleTimer, CallbackTimers
from utils_ak.architecture.func import PrefixHandler
from utils_ak.strtypes import cast_unicode

logger = logging.getLogger(__name__)

from utils_ak.message_queue.brokers import BrokerManager
from utils_ak.serialization.serializer import JsonSerializer

TIME_EPS = 0.001


# todo: make failure processing better

class BaseMicroservice(object):
    """ Microservice base class with timers and subscriber. Works on asyncio. """

    def __init__(self, logger=None, serializer=None, default_broker='zmq', brokers_config=None, asyncio_support=True):
        # aio
        self.tasks = []
        # sync
        self.timers = []

        # {collection: callback}
        self.callbacks = {}

        self.broker_manager = BrokerManager(default_broker, brokers_config)

        # {broker: f'{collection}::{topic}'}
        self.subscribed_to = {}

        self.logger = logger or logging.getLogger('-'.join([os.path.basename(sys.argv[0])]))

        self.default_exception_timeout = 10.
        self.max_exception_timeout = 3600
        self.fail_count = 0

        self.is_active = True

        self.serializer = serializer or JsonSerializer()

        self.asyncio_support = asyncio_support

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

    def subscribe(self, collection, topic, broker=None):
        self.broker_manager.subscribe(collection, topic, broker)

    def add_callback(self, collection, topic, callback=None, broker=None, formatter='default',
                     filter=None, topic_formatter=cast_unicode):
        self.broker_manager.subscribe(collection, topic, broker)
        self._add_callback(collection, topic, callback, formatter, filter, topic_formatter)

    def _add_callback(self, collection, topic, callback=None,
                      formatter='default', filter=None, topic_formatter=cast_unicode):
        assert type(topic) == str, 'Topic must be str'

        if formatter == 'default':
            formatter = self._args_formatter

        # todo: change to topic handler!!! Be careful here
        topic_handler = self.callbacks.setdefault(collection, PrefixHandler())
        topic_handler.set_topic_formatter(topic_formatter)
        topic_handler.add(topic, callback=callback, filter=filter, formatter=formatter)
        return topic_handler

    def publish(self, collection, topic, msg, broker=None):
        self.broker_manager.publish(collection, topic, msg, broker)

    def publish_json(self, collection, topic, msg, broker=None):
        self.publish(collection, topic, self.serializer.encode(msg), broker)

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

    def wrap_coroutine_broker(self, broker, timeout=0.01):
        is_async = self.broker_manager.support_async(broker)

        async def f():
            while True:
                try:
                    if is_async:
                        received = await self.broker_manager.aiopoll(broker)
                    else:
                        received = self.broker_manager.poll(timeout, broker)

                    if not received:
                        await asyncio.sleep(0)
                        continue

                    collection, topic, msg = received
                    try:
                        self.logger.debug(f'Received new message from {broker} broker',
                                          custom={'topic': topic, 'msg': msg})
                        await self.callbacks[collection].aiocall(topic, msg)

                        if self.fail_count != 0:
                            self.logger.info('Success. Resetting the failure counter')
                            self.fail_count = 0

                    except Exception as e:
                        self.on_exception(e, f"Exception occurred at the broker {broker} callback")
                except Exception as e:
                    self.on_exception(e, f'Broker {broker} failed to receive the message')

                if not self.is_active:
                    return

                await asyncio.sleep(0)

        return f()

    def _aiorun(self):
        self.loop = asyncio.get_event_loop()
        # self.loop.set_debug(True)
        self.logger.info('Microservice started')

        for timer in self.timers:
            self.tasks.append(self.wrap_coroutine_timer(timer))

        for broker in self.broker_manager.brokers:
            if self.broker_manager.has_subscriptions(broker):
                self.tasks.append(self.wrap_coroutine_broker(broker))

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
                                timer.check()
                            except Exception as e:
                                self.on_exception(e, 'Exception occurred at the timer callback')
                            else:
                                success = True
                for broker in self.broker_manager.brokers:
                    if not self.broker_manager.has_subscriptions(broker):
                        continue

                    try:
                        received = self.broker_manager.poll(timeout, broker)
                        if not received:
                            continue
                        collection, topic, msg = received
                        try:
                            self.logger.debug(f'Received new message from {broker} broker', custom={'topic': topic, 'msg': msg})
                            self.callbacks[collection].call(topic, msg)
                        except Exception as e:
                            self.on_exception(e, f"Exception occurred at the broker {broker} callback")
                        else:
                            success = True

                    except Exception as e:
                        self.on_exception(e, 'Broker {broker_name} failed to receive the message')

                if not self.is_active:
                    self.logger.info('Microservice not active. Stopping')
                    break

            except Exception as e:
                self.on_exception(e, 'Global exception occurred')

            if success and self.fail_count != 0:
                self.logger.info('Success. Resetting the failure counter')
                self.fail_count = 0

    def run(self):
        if self.asyncio_support:
            self._aiorun()
        else:
            self._run()

    def on_exception(self, e, msg):
        # todo: make properly
        self.logger.error(f'{msg} {e}')
        traceback.print_exc()
        to_sleep = min(self.default_exception_timeout * 2 ** (self.fail_count - 1), self.max_exception_timeout)
        time.sleep(to_sleep)
        self.fail_count += 1
