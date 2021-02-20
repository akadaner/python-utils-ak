from utils_ak.zmq import ZMQClient, endpoint

client = ZMQClient()


def publisher():
    for i in range(10):
        client.publish(endpoint("*", 5555), "topic", b"msg")


def subscriber():
    client.subscribe(endpoint("localhost", 5555))
    for i in range(10):
        received = None
        while not received:
            received = client.poll()
        print(received)


if __name__ == "__main__":
    import multiprocessing
    import time

    multiprocessing.Process(target=subscriber).start()
    time.sleep(1)
    multiprocessing.Process(target=publisher).start()
