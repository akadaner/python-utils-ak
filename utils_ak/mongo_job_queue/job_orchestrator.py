import time
import threading

from utils_ak.mongoengine import *
from .models import Job, Worker
import logging

from utils_ak.mongo_job_queue.worker.monitor import MonitorActor
from utils_ak.simple_microservice import SimpleMicroservice


class JobOrchestrator:
    def __init__(self, worker_controller, message_broker):
        self.timeout = 1
        self.controller = worker_controller
        self.ms = SimpleMicroservice('JobOrchestrator', message_broker=message_broker)
        self.monitor = MonitorActor(self.ms)
        self.process_active_jobs()
        self.ms.add_timer(self.process_new_jobs, 1.0)
        self.ms.add_callback('monitor_out', '', self.on_monitor)

    def run(self):
        self.ms.run()

    def process_active_jobs(self):
        pass # todo: go through all active jobs and process them

    def process_new_jobs(self):
        new_jobs = Job.objects(workers__size=0).all()

        if new_jobs:
            self.ms.logger.info(f'Processing {len(new_jobs)} new jobs')

        for new_job in new_jobs:
            worker_model = Worker()
            worker_model.save()
            new_job.workers.append(worker_model)
            new_job.save()
            self.ms.logger.info(f'Starting new worker {worker_model.id} {new_job.type} {new_job.payload}')
            self.controller.start_worker(str(worker_model.id), new_job.type, new_job.payload)

    def on_monitor(self, topic, msg):
        self.ms.logger.info(('On monitor', topic, msg))
