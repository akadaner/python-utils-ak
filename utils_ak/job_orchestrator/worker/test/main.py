import fire
from functools import partial
from utils_ak.job_orchestrator.worker.test.test_worker import TestWorker
from utils_ak.job_orchestrator.worker.worker import *

if __name__ == "__main__":
    fire.Fire(partial(run_worker, worker_cls=TestWorker))
