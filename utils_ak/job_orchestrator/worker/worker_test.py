import time
from loguru import logger
from utils_ak.simple_microservice import run_listener_async


def test_microservice_worker(worker_cls, payload):
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout("DEBUG")
    run_listener_async("monitor_in", message_broker=payload["message_broker"])
    time.sleep(2)
    worker = worker_cls("worker_id", payload)
    worker.run()
    logger.info("Finished batch")


def test_microservice_worker_deployment(deployment_fn, message_broker):
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout("DEBUG")
    run_listener_async("monitor_in", message_broker=message_broker)

    from utils_ak.deployment import DockerController

    ctrl = DockerController()

    import anyconfig

    deployment = anyconfig.load(deployment_fn)

    ctrl.stop(deployment["id"])
    ctrl.start(deployment)
    time.sleep(5)
    ctrl.stop(deployment["id"])
