from datetime import datetime

from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.coder.coders.json import cast_dict_or_list

from utils_ak.job_orchestrator.config import BASE_DIR

from .models import Job, Worker


# todo: retry


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker, timeout=600):
        self.timeout = 1
        self.controller = deployment_controller
        self.microservice = SimpleMicroservice(
            "JobOrchestrator", message_broker=message_broker
        )
        self._process_active_jobs()
        self.microservice.add_timer(self._process_new_jobs, 1.0)

        # todo: ping first message for zmq properly working. # todo: why needed though?
        self.microservice.add_timer(
            self.microservice.publish,
            interval=1,
            n_times=1,
            args=("job_orchestrator", "ping"),
        )
        self.microservice.add_callback("monitor_out", "status_change", self._on_monitor)
        self.timeout = timeout

    def run(self):
        self.microservice.run()

    def _process_active_jobs(self):
        pass

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
            new_job.locked_by = worker_model
            new_job.locked_at = datetime.utcnow()
            new_job.save()

    def _update_job_status(
        self, worker, old_worker_status, new_worker_status, response
    ):
        logger.info("Successfully processed job", id=worker.job.id)
        worker.response = response
        self.microservice.publish(
            "job_orchestrator",
            "status_change",
            id=str(worker.job.id),
            old_status=worker.job.status,
            new_status=new_worker_status,
            response=response,
        )
        worker.job.status = new_worker_status
        worker.job.save()

    def _update_worker_status(self, worker, old_status, new_status, state):
        if new_status == "success":
            self._update_job_status(
                worker, old_status, "success", state.get("response", {})
            )
        elif new_status == "stalled":
            self._update_job_status(worker, old_status, "error", {"msg": "stalled"})
        elif new_status == "running":
            self._update_job_status(worker, old_status, "running", {})

        worker.status = new_status
        worker.save()

        if worker.status in ["success", "stalled"]:
            logger.info("Stopping worker", id=id)
            self.controller.stop(id)

    def _on_monitor(self, topic, id, old_status, new_status, state):
        try:
            worker = Worker.objects(pk=id).first()  # todo: check if missing
        except:
            logger.error("Failed to fetch worker", id=id)
            return

        # todo: test
        # check if locked by another worker
        if str(id) != worker.job.locked_by:
            logger.error(
                "Job is locked by another worker",
                worker_id=id,
                another_worker_id=worker.job.locked_by,
            )
            logger.info("Stopping worker", id=id)
            self.controller.stop(id)
            return

        # todo: test
        # check if timeout expired
        if (datetime.utcnow() - worker.job.locked_at).total_seconds() > self.timeout:
            logger.error(
                "Timeout expired", worker_id=id, locked_at=worker.job.locked_at
            )
            logger.info("Stopping worker", id=id)
            self.controller.stop(id)
            return

        self._update_worker_status(worker, old_status, new_status, state)
