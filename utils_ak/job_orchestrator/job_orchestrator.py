from .models import Job, Worker

from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.coder.coders.json import cast_dict_or_list

from utils_ak.job_orchestrator.config import BASE_DIR


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker):
        self.timeout = 1
        self.controller = deployment_controller
        self.microservice = SimpleMicroservice(
            "JobOrchestrator", message_broker=message_broker
        )
        self._process_active_jobs()
        self.microservice.add_timer(self._process_new_jobs, 1.0)
        self.microservice.add_callback("monitor_out", "status_change", self._on_monitor)

    def run(self):
        self.microservice.run()

    def _process_active_jobs(self):
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

    def _process_new_jobs(self):
        new_jobs = Job.objects(workers__size=0).all()

        if new_jobs:
            self.microservice.logger.info(f"Processing {len(new_jobs)} new jobs")

        for new_job in new_jobs:
            worker_model = self._create_worker_model(new_job)
            deployment = self._create_deployment(worker_model)
            self.microservice.logger.info("Starting new worker", deployment=deployment)
            self.controller.start(deployment)
            new_job.status = "initializing"
            new_job.save()

    def _on_monitor(self, topic, id, old_status, new_status, state):
        try:
            worker = Worker.objects(pk=id).first()  # todo: check if missing
        except:
            logger.error("Failed to fetch worker", id=id)
            return

        if new_status == "success":
            worker.response = state.get("response")
            old_job_status = worker.job
            self.microservice.publish(
                "job_orchestrator",
                "status_change",
                id=str(worker.job.id),
                old_status=old_job_status,
                new_status="success",
                response=state.get("response"),
            )
            worker.job.save()

        worker.status = new_status
        worker.save()

        if worker.status == "success":
            logger.info("Stopping worker", id=id)
            self.controller.stop(id)
