from qset_feature_store.imports.runtime import *

from utils_ak.job_orchestrator.models import *
from utils_ak.job_orchestrator.job_orchestrator import JobOrchestrator
from utils_ak.job_orchestrator.tests.test_monitor import run_monitor
from utils_ak.job_orchestrator.tests.config.config import config


def run_job_orchestrator():
    utils.configure_loguru_stdout("DEBUG")

    connect(host=config.MONGODB_HOST, db=config.MONGODB_DB)
    logger.info("Connected to mongodb")
    controller = utils.ProcessController()

    utils.run_listener_async("job_orchestrator", message_broker=config.TRANSPORT)
    job_orchestrator = JobOrchestrator(controller, config.TRANSPORT)
    multiprocessing.Process(target=run_monitor).start()
    logger.info("Running job orchestrator...")
    job_orchestrator.run()


if __name__ == "__main__":
    run_job_orchestrator()
