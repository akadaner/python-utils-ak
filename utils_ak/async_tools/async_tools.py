# mechanisms with async_tools
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
from functools import wraps
import threading
from functools import partial
import time


# todo: implement other backends

def async_(f):
    @wraps(f)
    def inner(*args, **kwargs):
        thread = threading.Thread(target=f, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return inner


def run_async(f, *args, **kwargs):
    return async_(f)(*args, **kwargs)


def run_pool(callbacks, n_processes=4, Backend=ThreadPool):
    if len(callbacks) == 0:
        return []
    elif len(callbacks) == 1:
        # run in main process
        return callbacks[0]()
    else:
        n_processes = n_processes if len(callbacks) >= n_processes else len(callbacks)
        pool = Backend(n_processes)
        multiple_results = [pool.apply_async(f) for f in callbacks]
        res = [res.get() for res in multiple_results]
        pool.close()
        pool.join()
        return res


def run_map(f, args, n_processes=4, Backend=ThreadPool):
    return run_pool([partial(f, arg) for arg in args], n_processes, Backend)


def test():
    @async_
    def f(i):
        time.sleep(1)
        print(i)


    for i in range(10):
        f(i)

    print('Without join')

    threads = []
    for i in range(10):
        threads.append(f(i))
    for thread in threads:
        thread.join()

    print('With join')

    print('Run map')
    print(run_map(lambda x: x ** 2, [1,2,3]))

    print('Run pool')
    from functools import partial
    callbacks = [partial(lambda x: x ** 2, i) for i in [1,2,3]]
    print(run_pool(callbacks))

    def raise_exception(msg):
        raise Exception(msg)

    try:
        run_map(raise_exception, [1, 2, 3])
    except Exception as e:
        print(e)


if __name__ == '__main__':
    test()