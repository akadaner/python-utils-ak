from .models import Job, Worker

from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.coder.coders.json import cast_dict_or_list

from utils_ak.mongo_job_queue.monitor import MonitorActor
from utils_ak.mongo_job_queue.config import BASE_DIR


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker):
        self.timeout = 1
        self.controller = deployment_controller
        self.ms = SimpleMicroservice("JobOrchestrator", message_broker=message_broker)
        self.monitor = MonitorActor(self.ms)
        self.process_active_jobs()
        self.ms.add_timer(self.process_new_jobs, 1.0)
        self.ms.add_callback("monitor_out", "status_change", self.on_monitor_out)

    def run(self):
        self.ms.run()

    def process_active_jobs(self):
        pass  # todo: go through all active jobs and process them

    def _create_worker_model(self, job):
        worker_model = Worker()
        worker_model.job = job
        worker_model.save()
        job.workers.append(worker_model)
        job.save()
        return worker_model

    def _create_deployment(self, worker_model):
        deployment = cast_dict_or_list(
            os.path.join(BASE_DIR, "worker/deployment.yml.template")
        )

        params = {
            "deployment_id": str(worker_model.id),
            "payload": worker_model.job.payload,
            "image": worker_model.job.image,
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

    def on_monitor_out(self, topic, id, old_status, new_status):
        worker = Worker.objects(pk=id).first()  # todo: check if missing
        worker.status = new_status
        if worker.status == "success":
            worker.response = self.monitor.workers[id]["state"]["response"]
        worker.save()

        if worker.status == "success":
            self.controller.stop(id)
