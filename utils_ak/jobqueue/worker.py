import time
import json
from multiprocessing import Process
from threading import Thread
from utils_ak.jobqueue.mjq import MongoJobQueue
from utils_ak.tqdm_ak import tqdm_ak

import logging


def run(workers, is_async=False, update_timeout=1., auto_repair=True, use_threads=False, show_size=True):
    processes = []

    queue = cast_queue(workers[0].queue)

    for worker in workers:
        process = Process(target=worker.run) if not use_threads else Thread(target=worker.run)
        processes.append(process)

    for process in processes:
        process.start()

    if is_async:
        return processes

    # {'pending': 28, 'locked': 8, 'error': 0, 'success': 67}
    if show_size:
        pbar = tqdm_ak(total=queue.size())
    else:
        pbar = tqdm_ak()

    pbar.backend.set_description('MJQ')

    cur_finished = 0
    has_finished = False
    while True:
        if auto_repair:
            queue.repair()

        if all([not process.is_alive() for process in processes]):
            has_finished = True
        state = queue.get_state()
        finished = state['error'] + state['success']
        pbar.backend.update(max(finished - cur_finished, 0))
        cur_finished = finished
        pbar.backend.set_postfix(state)

        if has_finished:
            break

        time.sleep(update_timeout)

    pbar.backend.close()

    for process in processes:
        process.join()

    return processes


def cast_queue(queue_obj):
    if isinstance(queue_obj, MongoJobQueue):
        return queue_obj

    try:
        if isinstance(queue_obj, (list, tuple)) and len(queue_obj) == 2:
            return MongoJobQueue(queue_obj, max_attempts=3)
    except:
        pass

    raise Exception('Unknown queue object')


class Worker(object):
    def __init__(self, queue_obj, worker_id=None, target=None, args=None, kwargs=None, parse_payload=True, exception_default_timeout=5.):
        """
        :param queue_obj: `MongoJobQueue` or MongoJobQueue connection - tuple with
        :param logger: andy _logging instance
        :param worker_id: str or None
        :param target: callback. `Worker.process` is used by default
        :param args: arguments for target function
        :param kwargs: keyword arguments for target function
        :param parse_payload: parse or not message from the queue and pass them to target function as kwargs
        :param exception_default_timeout: double. error timeout
        """
        self.queue = queue_obj
        self.worker_id = worker_id
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.parse_payload = parse_payload

        self.fail_count = 0
        self.exception_default_timeout = exception_default_timeout

        self.target = target or self.process

        self.logger = None

        self.no_jobs_timeout = 1

    def init_logging(self):
        self.logger = logging.getLogger(f'MJQLogger-{self.worker_id}' if self.worker_id is not None else 'MJQLogger')

    def connect(self):
        self.queue = cast_queue(self.queue)

    def process(self, *args, **kwargs):
        raise NotImplementedError('Not implemented')

    def _process(self, job):
        msg = job.payload
        kwargs = dict(self.kwargs)
        if self.parse_payload:
            kwargs.update(json.loads(msg))
            return self.target(*self.args, **kwargs)
        else:
            return self.target(msg, *self.args, **self.kwargs)

    def on_receive(self, job):
        self.logger.info('Processing {}'.format(job.payload))

    def on_finish(self):
        self.logger.info('Finished')
        return True

    def on_complete(self, job):
        job.complete()
        self.logger.info(f"Completed {job.payload}")
        self.fail_count = 0

    def on_exception(self, job, e):
        self.fail_count += 1

        try:
            job.error()
        except:
            pass

        to_sleep = min(self.exception_default_timeout * 2 ** (self.fail_count - 1), 3600.)
        self.logger.exception(f'Error on payload: {job.payload}. Sleeping for {to_sleep}')
        time.sleep(to_sleep)

    def run(self):
        self.init_logging()
        self.connect()

        while True:
            try:
                job = self.queue.get(consumer_id=self.worker_id)

                if not job:
                    to_exit = self.on_finish()
                    if to_exit:
                        return
                    else:
                        time.sleep(self.no_jobs_timeout)
                        continue

                try:
                    self.on_receive(job)
                    self._process(job)
                    self.on_complete(job)

                except Exception as e:
                    self.on_exception(job, e)

            except Exception as e:
                self.logger.error('Global error. Stopping worker...', e)
                return


if __name__ == '__main__':
    # {'pending': 28, 'locked': 8, 'error': 0, 'success': 67}
    pbar = tqdm_ak(total=20)
    pbar.backend.set_description('MJQ')

    pbar.backend.update(1)

    time.sleep(1)
    pbar.backend.update(-1)
    time.sleep(1)

    pbar.backend.close()
    pbar = tqdm_ak(total=20)
