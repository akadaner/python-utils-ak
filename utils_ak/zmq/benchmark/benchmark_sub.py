import time
import zmq

from multiprocessing import Process

URL = "tcp://127.0.0.1:5556"
SYSTEM_URL = "tcp://127.0.0.1:5557"
POLL = True
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


def throughput_sink():
    context = zmq.Context()
    # router_system - server analogue
    router_system = context.socket(zmq.ROUTER)
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"")

    #  Add your router_system options here.
    #  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

    if POLL:
        poller = zmq.Poller()
        poller.register(sub)

    router_system.bind(SYSTEM_URL)
    sub.bind(URL)

    time.sleep(0.1)

    msg = router_system.recv_multipart()
    assert msg[1] == b"BEGIN", msg
    count = int(msg[2].decode("ascii"))
    router_system.send_multipart(msg)

    flags = zmq.NOBLOCK if POLL else 0

    counter = 0
    while counter != count:
        if POLL:
            socks = dict(poller.poll())
            if sub in socks and socks[sub] == zmq.POLLIN:
                sub.recv_multipart(flags=flags, copy=COPY)
                counter += 1
        else:
            sub.recv_multipart(flags=flags, copy=COPY)
            counter += 1

    # msg[0] - instance to which we send message!
    router_system.send_multipart([msg[0], b"DONE"])

    router_system.close()
    sub.close()
    context.term()


def get_throughput():
    context = zmq.Context()
    # dealer - client analogue
    system_dealer = context.socket(zmq.DEALER)
    pub = context.socket(zmq.PUB)

    # no memory limits for sending
    system_dealer.SNDHWM = 0
    pub.SNDHWM = 0

    #  Add your socket options here.
    #  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

    system_dealer.connect(SYSTEM_URL)
    pub.connect(URL)

    time.sleep(0.1)
    data = b" " * SIZE

    flags = 0
    system_dealer.send_multipart([b"BEGIN", str(COUNT).encode("ascii")])
    # Wait for the other side to connect.
    msg = system_dealer.recv_multipart()
    assert msg[0] == b"BEGIN"
    start = now()
    for i in range(COUNT):
        pub.send_multipart([b"", data], flags=flags, copy=COPY)
    sent = now()
    # wait for receiver
    reply = system_dealer.recv_multipart()
    elapsed = now() - start
    assert reply[0] == b"DONE"
    send_only = sent - start

    send_throughput = COUNT / send_only
    throughput = COUNT / elapsed
    megabits = throughput * SIZE * 8 / 1e6

    print("Throughput")
    print(f"Message size   : {SIZE}     [B]")
    print(f"Message count  : {COUNT}     [msgs]")
    print(f"Send only      : {send_throughput}     [msg/s]")
    print(f"Mean throughput: {throughput}     [msg/s]")
    print(f"Mean throughput: {megabits} [Mb/s]")
    print(f"Test time      : {elapsed} [s]")
    print()
    context.destroy()
    return (send_throughput, throughput)


def benchmark_throughput():
    p = Process(target=throughput_sink)
    p.start()
    get_throughput()
    p.join()


if __name__ == "__main__":
    benchmark_throughput()
