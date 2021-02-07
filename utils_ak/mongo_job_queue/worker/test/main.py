import fire
from utils_ak.mongo_job_queue.worker.test.test_worker import TestWorker
from utils_ak.serialization import cast_dict_or_list


def main(config):
    config = cast_dict_or_list(config)
    worker = TestWorker(config['worker_id'], config['payload'], config['message_broker'])
    worker.run()


if __name__ == '__main__':
    fire.Fire(main)
