from datetime import datetime

from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.coder.coders.json import cast_dict_or_list

from utils_ak.job_orchestrator.config import BASE_DIR

from .models import Job, Worker


# todo: retry


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker):
        self.timeout = 1
        self.controller = deployment_controller
        self.microservice = SimpleMicroservice(
            "JobOrchestrator", message_broker=message_broker
        )
        self.microservice.add_timer(self._process_new_jobs, 1.0)
        self.microservice.add_timer(self._process_initializing_jobs, 5.0)

        # todo: ping first message for zmq properly working. # todo: why needed though?
        self.microservice.add_timer(
            self.microservice.publish,
            interval=1,
            n_times=1,
            args=("job_orchestrator", "ping"),
        )
        self.microservice.add_callback("monitor_out", "status_change", self._on_monitor)

    def run(self):
        self.microservice.run()

    def _process_initializing_jobs(self):
        initializing_jobs = Job.objects(status="initializing").all()

        if initializing_jobs:
            self.microservice.logger.info(
                "Processing initializing jobs", n_jobs=len(initializing_jobs)
            )

        for job in initializing_jobs:
            worker = job.locked_by
            assert worker is not None, "Worker not assigned for the job"
            if (
                datetime.utcnow() - worker.job.locked_at
            ).total_seconds() > job.initializing_timeout:
                logger.error(
                    "Timeout expired",
                    worker_id=worker.id,
                    locked_at=worker.job.locked_at,
                )
                self._update_worker_status(
                    worker,
                    worker.status,
                    "error",
                    {"response": cast_js({"msg": "Timeout expired"})},
                )
                return

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
            "image": worker_model.job.runnable.get("image", ""),
            "main_file_path": worker_model.job.runnable.get("main_file_path", ""),
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
                worker, old_status, "success", state.get("response", "")
            )
        elif new_status == "stalled":
            self._update_job_status(
                worker, old_status, "error", cast_js({"msg": "stalled"})
            )
        elif new_status == "running":
            self._update_job_status(worker, old_status, "running", "")
        elif new_status == "error":
            self._update_job_status(worker, old_status, "error", "")

        worker.status = new_status
        worker.save()

        if worker.status in ["success", "stalled", "error"]:
            logger.info("Stopping worker", id=worker.id)
            self.controller.stop(str(worker.id))

    def _on_monitor(self, topic, id, old_status, new_status, state):
        try:
            worker = Worker.objects(pk=id).first()  # todo: check if missing
        except:
            logger.error("Failed to fetch worker", id=id)
            return

        # check if locked by another worker
        if str(id) != str(worker.job.locked_by.id):
            logger.error(
                "Job is locked by another worker",
                worker_id=id,
                another_worker_id=worker.job.locked_by.id,
            )
            logger.info("Stopping worker", id=id)
            self.controller.stop(id)
            return

        self._update_worker_status(worker, old_status, new_status, state)
