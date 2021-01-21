import time
import zmq

from multiprocessing import Process

URL = 'tcp://127.0.0.1:5556'
SYSTEM_URL = 'tcp://127.0.0.1:5557'
POLL = True
# copy=True is much faster
COPY = True
SIZE = 1024
COUNT = 100_000
# lat or thr
now = time.perf_counter

from utils_ak.zmq.client import ZMQClient


# docs
# socket types: http://api.zeromq.org/4-1:zmq-socket
# some options dock. http://api.zeromq.org/2-1:zmq-setsockopt

def throughput_sink():
    context = zmq.Context()
    # router_system - server analogue
    router_system = context.socket(zmq.ROUTER)

    client = ZMQClient(context=context)
    client.subscribe(URL, conn_type='connect')

    router_system.bind(SYSTEM_URL)

    time.sleep(0.2)

    system_msg = router_system.recv_multipart()
    assert system_msg[1] == b'BEGIN', system_msg
    count = int(system_msg[2].decode('ascii'))
    router_system.send_multipart(system_msg)

    counter = 0
    while counter != count:
        msg = client.poll(0.01)
        if msg:
            counter += 1

    # system_msg[0] - instance to which we send message!
    router_system.send_multipart([system_msg[0], b'DONE'])

    router_system.close()
    context.term()


def get_throughput():
    context = zmq.Context()
    # dealer - client analogue
    system_dealer = context.socket(zmq.DEALER)
    client = ZMQClient()

    # no memory limits for sending
    system_dealer.SNDHWM = 0

    #  Add your socket options here.
    #  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

    system_dealer.connect(SYSTEM_URL)

    time.sleep(0.2)
    data = b' ' * SIZE

    flags = 0
    system_dealer.send_multipart([b'BEGIN', str(COUNT).encode('ascii')])
    # Wait for the other side to connect.
    msg = system_dealer.recv_multipart()
    assert msg[0] == b'BEGIN'
    start = now()
    for i in range(COUNT):
        client.publish(URL, b'', data, conn_type='bind')
    sent = now()
    # wait for receiver
    reply = system_dealer.recv_multipart()
    elapsed = now() - start
    assert reply[0] == b'DONE'
    send_only = sent - start

    send_throughput = COUNT / send_only
    throughput = COUNT / elapsed
    megabits = throughput * SIZE * 8 / 1e6

    print('Throughput')
    print(f'Message size   : {SIZE}     [B]')
    print(f'Message count  : {COUNT}     [msgs]')
    print(f'Send only      : {send_throughput}     [msg/s]')
    print(f'Mean throughput: {throughput}     [msg/s]')
    print(f'Mean throughput: {megabits} [Mb/s]')
    print(f'Test time      : {elapsed} [s]')
    print()
    context.destroy()
    return (send_throughput, throughput)


def benchmark_throughput():
    p = Process(target=throughput_sink)
    p.start()
    get_throughput()
    p.join()


if __name__ == '__main__':
    benchmark_throughput()
