import time
import zmq

from multiprocessing import Process

URL = 'tcp://127.0.0.1:5555'
POLL = False
# copy=True is much faster
COPY = True
SIZE = 1024
COUNT = 1_000_000
# lat or thr
now = time.perf_counter

# If copy is unspecified (False in our case?), messages smaller than this many bytes will be copied and messages larger than this will be shared with libzmq.
# Should the frame(s) be sent in a copying or non-copying manner. If copy=False, frames smaller than self.copy_threshold bytes will be copied anyway.
# matters only for copy=false in my understanding!
zmq.COPY_THRESHOLD = 0


# docs
# socket types: http://api.zeromq.org/4-1:zmq-socket
# some options dock. http://api.zeromq.org/2-1:zmq-setsockopt

def latency_echo():
    """echo messages on a REP socket. Should be started before `latency` """
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    if POLL:
        poller = zmq.Poller()
        poller.register(socket)

    socket.bind(URL)

    block = zmq.NOBLOCK if POLL else 0

    for i in range(COUNT + 1):
        if POLL:
            res = poller.poll()
        msg = socket.recv(block, copy=COPY)

        if POLL:
            res = poller.poll()

        socket.send(msg, block, copy=COPY)

    msg = socket.recv()
    assert msg == b'done'

    socket.close()
    context.term()


def get_latency():
    """Perform a latency test"""
    context = zmq.Context()
    s = context.socket(zmq.REQ)
    # behaviour on socket close and term
    s.setsockopt(zmq.LINGER, -1)
    s.connect(URL)

    if POLL:
        poller = zmq.Poller()
        poller.register(s)

    msg = b' ' * SIZE

    block = zmq.NOBLOCK if POLL else 0
    # trigger one roundtrip before starting the timer
    s.send(msg)
    s.recv()
    start = now()

    for i in range(COUNT):
        if POLL:
            res = poller.poll()
            assert (res[0][1] & zmq.POLLOUT)
        s.send(msg, block, copy=COPY)

        if POLL:
            res = poller.poll()
            assert (res[0][1] & zmq.POLLIN)
        msg = s.recv(block, copy=COPY)

        assert len(msg) == SIZE

    elapsed = now() - start

    s.send(b'done')

    latency = 1e6 * elapsed / (COUNT * 2.)

    print('Latency')
    print(f'Message size   : {SIZE}     [B]')
    print(f'Roundtrip count: {COUNT}     [msgs]')
    print(f'Mean latency   : {latency} [µs]')
    print(f'Test time      : {elapsed} [s]')
    print()

    context.destroy()
    return latency


def throughput_sink():
    context = zmq.Context()
    # router - server analogue
    socket = context.socket(zmq.ROUTER)
    # no memory limits for receiving
    socket.RCVHWM = 0

    #  Add your socket options here.
    #  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

    if POLL:
        poller = zmq.Poller()
        poller.register(socket)

    socket.bind(URL)
    msg = socket.recv_multipart()
    assert msg[1] == b'BEGIN', msg
    count = int(msg[2].decode('ascii'))
    socket.send_multipart(msg)

    flags = zmq.NOBLOCK if POLL else 0

    for i in range(count):
        if POLL:
            res = poller.poll()
            assert (res[0][1] & zmq.POLLIN)
        msg = socket.recv_multipart(flags=flags, copy=COPY)

    # msg[0] - instance to which we send message!
    socket.send_multipart([msg[0], b'DONE'])

    socket.close()
    context.term()


def get_throughput():
    context = zmq.Context()
    # dealer - client analogue
    socket = context.socket(zmq.DEALER)
    # no memory limits for sending
    socket.SNDHWM = 0

    #  Add your socket options here.
    #  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

    if POLL:
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLOUT)

    socket.connect(URL)
    data = b' ' * SIZE

    flags = zmq.NOBLOCK if POLL else 0
    socket.send_multipart([b'BEGIN', str(COUNT).encode('ascii')])
    # Wait for the other side to connect.
    msg = socket.recv_multipart()
    assert msg[0] == b'BEGIN'
    start = now()
    for i in range(COUNT):
        if POLL:
            res = poller.poll()
            assert (res[0][1] & zmq.POLLOUT)
        socket.send(data, flags=flags, copy=COPY)
    sent = now()
    # wait for receiver
    reply = socket.recv_multipart()
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


def benchmark_latency():
    p = Process(target=latency_echo)
    p.start()
    get_latency()
    p.join()


if __name__ == '__main__':
    benchmark_throughput()
    benchmark_latency()
