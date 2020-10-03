from queue import Queue
import asyncio
import concurrent.futures

from utils_ak.architecture.finite_state_machine import FiniteStateMachine
from utils_ak.architecture.sink import ReturnValuesSink

import logging

logger = logging.getLogger(__name__)


class TaskOperation(FiniteStateMachine):
    _states = ['blank', 'initiated', 'started', 'failed', 'finished']

    def __init__(self):
        super().__init__()
        # task is anything
        self.tasks = []

    def on_initiated(self):
        pass

    def on_failed(self):
        pass

    def on_finished(self):
        pass

    def on_started(self):
        pass

    def check_state(self):
        raise NotImplementedError

    def initiate(self, tasks):
        self.tasks = tasks
        self.set_state('initiated')

    def start(self):
        self.set_state('started')

    def finish(self):
        self.set_state('finished')

    def fail(self):
        self.set_state('failed')


class AsyncioTaskOperation(TaskOperation):
    def __init__(self, results_sink=None, integrity_guard=None, max_workers=20, operation_name=None, max_retries=10):
        super().__init__()
        self.fired_tasks_queue = Queue()
        self.tasks_queue = Queue()

        self.integrity_guard = integrity_guard
        self.results_sink = results_sink or ReturnValuesSink()

        self.max_workers = max_workers
        self.operation_name = operation_name

        self.fail_alert = False

        self.max_retries = max_retries

        # {task_name: number_of_retries}
        self.retry_counter = {}

    def initiate(self, tasks):
        self.tasks = tasks
        for task in tasks:
            self.tasks_queue.put(task)
            self.retry_counter[task.name] = 0
        self.set_state('initiated')

    def check_integrity(self):
        if not self.integrity_guard:
            return True

        if not self.integrity_guard.is_integral():
            self.set_state('failed')
            return False
        return True

    def on_state_change(self, old_state, new_state):
        super().on_state_change(old_state, new_state)

        if self.integrity_guard:
            if new_state in ['failed', 'finished']:
                self.integrity_guard.leave_unstable_state()
            elif new_state in ['started']:
                self.integrity_guard.enter_unstable_state()

    async def execute(self):
        if not self.check_integrity():
            return

        self.set_state('started')

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        loop = asyncio.get_event_loop()
        loop.set_default_executor(executor)

        while not self.tasks_queue.empty():
            while not self.tasks_queue.empty():
                if self.fail_alert:
                    break

                task = self.tasks_queue.get()

                self.fired_tasks_queue.put(loop.run_in_executor(executor, self._execute_task, task))

                self.tasks_queue.task_done()

            if self.fail_alert:
                break

            while not self.fired_tasks_queue.empty():
                await self.fired_tasks_queue.get()

        self.set_state('failed' if self.fail_alert else 'finished')

    def _execute_task(self, task):
        try:
            res = task()
        except Exception as e:
            self.retry_counter[task.name] += 1

            if self.retry_counter[task.name] >= self.max_retries:
                self.fail_alert = True
            else:
                self.tasks_queue.put(task)
            logger.warning(f'Failed to process task, error: {e}')
        else:
            if self.results_sink:
                self.results_sink.store(task.name, res)

    def get_results(self):
        return self.results_sink.get_results()


if __name__ == '__main__':
    import time
    from utils_ak.logging import configure_stream_logging

    configure_stream_logging(level=logging.DEBUG)

    from utils_ak.architecture.integrity_guard import FilesystemIntegrityGuard


    def gen_runnable(name):
        class Task:
            def __init__(self):
                self.name = name

            def __call__(self, *args, **kwargs):
                logger.info(f'Runnable {name} started...')
                time.sleep(1)
                if name == 4:
                    raise Exception('Fail example')
                logger.info(f'Runnable {name} finished...')

        return Task()


    operation = AsyncioTaskOperation(integrity_guard=FilesystemIntegrityGuard())
    operation.initiate([gen_runnable(i) for i in range(5)])
    asyncio.get_event_loop().run_until_complete(operation.execute())
