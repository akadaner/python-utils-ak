import fire
from functools import partial

from utils_ak.loguru import configure_loguru_stdout

from utils_ak.job_orchestrator.worker.sample_worker.sample_worker import SampleWorker
from utils_ak.job_orchestrator.worker.worker import *

if __name__ == "__main__":
    configure_loguru_stdout()
    fire.Fire(partial(run_worker, worker_cls=SampleWorker))
