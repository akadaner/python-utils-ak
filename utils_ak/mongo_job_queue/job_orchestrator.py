import time
import threading

from utils_ak.mongoengine import *
from .models import Job, Worker
import logging

from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.serialization import cast_dict_or_list

from utils_ak.mongo_job_queue.monitor import MonitorActor
from utils_ak.mongo_job_queue.config import BASE_DIR


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker):
        self.timeout = 1
        self.controller = deployment_controller
        self.ms = SimpleMicroservice("JobOrchestrator", message_broker=message_broker)
        # todo: make properly
        self.monitor = MonitorActor(self.ms)
        self.process_active_jobs()
        self.ms.add_timer(self.process_new_jobs, 1.0)
        self.ms.add_callback("monitor_out", "", self.on_monitor_out)

    def run(self):
        self.ms.run()

    def process_active_jobs(self):
        pass  # todo: go through all active jobs and process them

    def _create_worker_model(self, job):
        worker_model = Worker()
        worker_model.save()
        job.workers.append(worker_model)
        job.save()
        return worker_model

    def _create_deployment(self, worker_model):
        # generate deployment
        deployment = cast_dict_or_list(
            os.path.join(BASE_DIR, "worker/deployment.yml.template")
        )

        # todo: Hardcode, use new_job.type
        IMAGE = "akadaner/test-worker"
        # todo: hardcode, use generic message broker
        MESSAGE_BROKER = [
            "zmq",
            {
                "endpoints": {
                    "monitor": {
                        "endpoint": "tcp://host.k3d.internal:5555",
                        "type": "sub",
                    }
                }
            },
        ]
        # MESSAGE_BROKER = ['zmq', {'endpoints': {'monitor': {'endpoint': 'tcp://host.docker.internal:5555', 'type': 'sub'}}}]

        params = {
            "deployment_id": str(worker_model.id),
            "payload": worker_model.job.payload,
            "image": IMAGE,
            "message_broker": MESSAGE_BROKER,
        }
        deployment = fill_template(deployment, **params)

        return deployment

    def process_new_jobs(self):
        new_jobs = Job.objects(workers__size=0).all()

        if new_jobs:
            self.ms.logger.info(f"Processing {len(new_jobs)} new jobs")

        for new_job in new_jobs:
            worker_model = self._create_worker_model(new_job)
            deployment = self._create_deployment(worker_model)

            self.ms.logger.info("Starting new worker", deployment=deployment)

            self.controller.start(deployment)

    def on_monitor_out(self, topic, msg):
        self.ms.logger.info("On monitor out", topic=str(topic), msg=str(msg))
        if topic == "status_change":
            worker = Worker.objects(pk=msg["id"]).first()  # todo: check if missing
            worker.status = msg["new_status"]
            if worker.status == "success":
                worker.response = self.monitor.workers[msg["id"]]["state"]["response"]
            worker.save()

            if worker.status == "success":
                self.controller.stop(msg["id"])
