import time
from utils_ak import jobqueue as jq

mongodb_cs = 'mongodb+srv://arseniikadaner:Nash0lbanan@cluster0-2umoy.mongodb.net/test?retryWrites=true'
queue_name = 'mjq_test'
conn = (mongodb_cs, queue_name)


def process(a, b, worker_id='default'):
    print("{} doing something with {} {}".format(worker_id, a, b))
    time.sleep(1)


if __name__ == '__main__':
    queue = jq.MongoJobQueue(conn)
    queue.clear()
    print('Current queue state', queue.get_state())
    queue.put([{'a': i, 'b': i * 2} for i in range(100)], upsert=True)
    workers = [jq.Worker(conn, target=process, kwargs={'worker_id': i}) for i in range(5)]
    jq.run(workers, use_threads=False)

    print('Current queue state', queue.get_state())
    queue.clear()