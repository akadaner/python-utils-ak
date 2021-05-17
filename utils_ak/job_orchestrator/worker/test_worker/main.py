import fire
from functools import partial
from utils_ak.job_orchestrator.worker.test.test_worker import TestWorker
from utils_ak.job_orchestrator.worker.worker import *
from utils_ak.loguru import configure_loguru_stdout

if __name__ == "__main__":
    configure_loguru_stdout()
    fire.Fire(partial(run_worker, worker_cls=TestWorker))
