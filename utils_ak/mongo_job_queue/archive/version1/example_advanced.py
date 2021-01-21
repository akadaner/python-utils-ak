import time
from utils_ak import jobqueue as jq
from utils_ak.log import configure_logging
from utils_ak.os import make_directories
import logging

mongodb_cs = 'mongodb+srv://arseniikadaner:Nash0lbanan@cluster0-2umoy.mongodb.net/test?retryWrites=true'
queue_name = 'mjq_test'
conn = (mongodb_cs, queue_name)


class MyWorker(jq.Worker):
    def __init__(self, worker_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = 0
        self.worker_id = worker_id

    def process(self, a, b):
        self.logger.info("{}: doing something with {} {}".format(self.worker_id, a, b))
        self.counter += 1
        time.sleep(1)
        self.logger.info('{}: Total tasks: {}'.format(self.worker_id, self.counter))

    def init_logging(self):
        make_directories('logs/')
        configure_logging(file_stream=True, logs_path=f'logs/{self.worker_id}.log')
        self.logger = logging.getLogger(f'MJQLogger-{self.worker_id}' if self.worker_id is not None else 'MJQLogger')


if __name__ == '__main__':
    queue = jq.MongoJobQueue(conn)
    queue.clear()
    queue.put([{'a': i, 'b': i * 2} for i in range(10)], upsert=True) # default priority is 0
    queue.put([{'a': i * 1000, 'b': i * 2 * 1000} for i in range(10)], upsert=False, priority=2)
    workers = [MyWorker(i, conn) for i in range(2)]
    jq.run(workers, use_threads=False)
    print(queue.get_state())
    queue.clear()

