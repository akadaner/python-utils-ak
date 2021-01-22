import time
import threading

from utils_ak.mongoengine import *
from .models import Job, Worker
import logging


class JobOrchestrator:
    def __init__(self, executor_manager, executor_monitor):
        self.timeout = 1
        self.executor_manager = executor_manager
        self.executor_monitor = executor_monitor

    # todo: Run on init
    def process_active_jobs(self):
        pass # todo: go through all active jobs and process them

    # todo: run periodically
    def process_new_jobs(self):
        pass # todo: go through all new jobs and process them

    def init_executor(self, job):
        pass # todo: create executor database entity

    def disable_executor(self, executor):
        self.executor_manager.disable_executor(executor)
        pass # todo: change executor status

    def start_executor(self, executor):
        # todo: add timeout for failed start
        res = self.executor_manager.start_executor(executor)
        # todo: change executor state

    def on_executor_monitor(self, ts, topic, msg):
        pass  # process monitor messages
