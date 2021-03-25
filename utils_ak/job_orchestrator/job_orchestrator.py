from datetime import datetime

from utils_ak.loguru import *
from utils_ak.deployment import *
from utils_ak.simple_microservice import SimpleMicroservice
from utils_ak.dict import fill_template
from utils_ak.coder.coders.json import cast_dict_or_list

from .models import Job, Worker


# todo: retry


class JobOrchestrator:
    def __init__(self, deployment_controller, message_broker):
        self.controller = deployment_controller
        self.microservice = SimpleMicroservice(
            "JobOrchestrator", message_broker=message_broker
        )
        self.microservice.add_timer(self._process_pending, 1.0)
        self.microservice.add_timer(self._process_initializing, 5.0)
        self.microservice.add_timer(self._process_running, 5.0)
        self.microservice.add_callback("monitor_out", "status_change", self._on_monitor)
        self.microservice.register_publishers(["job_orchestrator"])

    def run(self):
        self.microservice.run()

    def _update_job_status(self, worker, status, response):
        logger.debug(
            "Updating job status", id=worker.job.id, status=status, response=response
        )

        if status == "success":
            logger.info(
                "Successfully processed job", id=worker.job.id, response=response
            )
        elif status == "error":
            logger.error("Failed to process job", id=worker.job.id, response=response)

        self.microservice.publish(
            "job_orchestrator",
            "status_change",
            id=str(worker.job.id),
            old_status=worker.job.status,
            new_status=status,
            response=response,
        )
        worker.response = response
        worker.job.status = status
        worker.job.save()

    def _update_worker_status(self, worker, status, state=None):
        logger.debug("Updating worker status", id=worker.id, status=status, state=state)
        state = state or {}
        self._update_job_status(worker, status, state.get("response", ""))

        worker.status = status
        worker.save()

        if worker.status in ["success", "stalled", "error"]:
            self._stop_worker(worker)

    def _stop_worker(self, worker):
        logger.info("Stopping worker", id=worker.id)
        self.controller.stop(str(worker.id))

    def _create_worker_model(self, job):
        worker_model = Worker()
        worker_model.job = job
        worker_model.save()
        job.workers.append(worker_model)
        job.locked_by = worker_model
        job.locked_at = datetime.utcnow()
        job.save()
        return worker_model

    def _create_deployment(self, worker_model):
        deployment = cast_dict_or_list(
            os.path.join(os.path.dirname(__file__), "worker/deployment.yml.template")
        )

        params = {
            "deployment_id": str(worker_model.id),
            "payload": worker_model.job.payload,
            "image": worker_model.job.runnable.get("image", ""),
            "python_main": worker_model.job.runnable.get("python_main", ""),
        }
        deployment = fill_template(deployment, **params)
        return deployment

    def _process_pending(self):
        jobs = Job.objects(status="pending").all()

        if jobs:
            self.microservice.logger.info("Processing: pending", n=len(jobs))

        for job in jobs:
            worker_model = self._create_worker_model(job)
            self._update_worker_status(worker_model, "initializing")
            deployment = self._create_deployment(worker_model)
            self.controller.start(deployment)

    def _process_initializing(self):
        jobs = Job.objects(status="initializing").all()

        if jobs:
            self.microservice.logger.info("Processing: initializing", n=len(jobs))

        for job in jobs:
            worker = job.locked_by
            assert worker is not None, "Worker not assigned for the job"
            if (
                job.initializing_timeout
                and (datetime.utcnow() - worker.job.locked_at).total_seconds()
                > job.initializing_timeout
            ):
                logger.error(
                    "Initializing timeout expired",
                    worker_id=worker.id,
                    locked_at=worker.job.locked_at,
                )
                self._update_worker_status(
                    worker,
                    "error",
                    {"response": "Initializing timeout expired"},
                )

    def _process_running(self):
        jobs = Job.objects(status="running").all()

        if jobs:
            self.microservice.logger.info("Processing: running", n=len(jobs))

        for job in jobs:
            worker = job.locked_by
            assert worker is not None, "Worker not assigned for the job"
            if (
                job.running_timeout
                and (datetime.utcnow() - worker.job.locked_at).total_seconds()
                > job.running_timeout
            ):
                logger.error(
                    "Running timeout expired",
                    worker_id=worker.id,
                    locked_at=worker.job.locked_at,
                )
                self._update_worker_status(
                    worker,
                    "error",
                    {"response": "Running timeout expired"},
                )

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
            self._stop_worker(worker)
            return

        status = new_status
        if status in ["stalled", "error"]:
            # todo: make properly
            if "response" not in state:
                state["response"] = status
            status = "error"
        self._update_worker_status(worker, status, state)
