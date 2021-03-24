import time
from loguru import logger
from utils_ak.simple_microservice import run_listener_async


def test_microservice_worker(worker_cls, payload, run_listener=True):
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout("DEBUG")
    if run_listener:
        run_listener_async("monitor_in", message_broker=payload["message_broker"])
    time.sleep(2)
    worker = worker_cls("worker_id", payload)
    worker.run()
    logger.info("Finished batch")


def test_microservice_worker_deployment(
    deployment_fn,
    controller,
    message_broker,
    run_listener=True,
):
    from utils_ak.loguru import configure_loguru_stdout

    configure_loguru_stdout("DEBUG")
    if run_listener:
        run_listener_async("monitor_in", message_broker=message_broker)
    import anyconfig

    deployment = anyconfig.load(deployment_fn)

    controller.stop(deployment["id"])
    controller.start(deployment)
    time.sleep(5)
    controller.stop(deployment["id"])
