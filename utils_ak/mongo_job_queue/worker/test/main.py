import fire
from utils_ak.mongo_job_queue.worker.test.test_worker import TestWorker
from utils_ak.serialization import cast_dict_or_list


def main(config=None):
    # config = cast_dict_or_list(config)
    docker_compose_config = cast_dict_or_list(r"C:\Users\Mi\Desktop\master\code\git\python-utils-ak\utils_ak\mongo_job_queue\data\docker-compose\601fb3b2fae079694e9bffc6\docker-compose.yml")
    config = cast_dict_or_list(docker_compose_config['services']['test-worker']['command'][1])
    print(config)
    exit(1)
    worker = TestWorker(config['worker_id'], config['payload'], config['message_broker'])
    worker.run()


if __name__ == '__main__':
    fire.Fire(main)
