from .topic_handler import TopicHandler
from .prefix_handler import PrefixHandler
from .handler import Handler

if __name__ == '__main__':
    def callback1(msg):
        print('callback1', msg)
        return 1


    def callback2(msg):
        print('callback2', msg)
        return 2


    def filter(msg):
        print('filter', msg)
        return 'bar1' in msg


    def formatter(msg):
        return (msg.split('.')[0],), {}


    handler = Handler(callback=callback1)
    print(handler('foo.bar1'))
    print(handler('foo.bar2'))
    print()

    handler = Handler(callback=[callback1, callback2], filter=filter)
    print(handler('foo.bar1'))
    print(handler('foo.bar2'))
    print()

    handler = Handler(callback=[callback1, callback2])
    handler.add_filter(filter, rule=lambda b: not b)
    print(handler('foo.bar1'))
    print(handler('foo.bar2'))
    print()

    handler = Handler(callback=[callback1, callback2], filter=filter, formatter=formatter)
    print(handler('foo.bar1'))
    print(handler('foo.bar2'))
    print()


    def topic_callback1(topic, msg):
        print('callback1', msg)
        return 1


    handler = TopicHandler()
    handler.add('a.b', callback=topic_callback1)
    handler('a', 'a')
    handler('b', 'b')
    handler('a.b', 'a.b')
    print()

    handler = PrefixHandler()
    handler.add('a', callback=topic_callback1)
    handler('a', 'a')
    handler('b', 'b')
    handler('a.b', 'a.b')
    print()

    # aio stuff
    import asyncio


    async def aiocallback(topic='', msg=''):
        await asyncio.sleep(0.5)
        print('aiocallback', topic, msg)
        return 3


    loop = asyncio.get_event_loop()

    handler = Handler(callback=[callback1, callback2, aiocallback], filter=filter)
    loop.run_until_complete(handler.aiocall('foo.bar1'))
    loop.run_until_complete(handler.aiocall('foo.bar2'))
    print()

    handler = TopicHandler()
    handler.add('a.b', callback=aiocallback)
    loop.run_until_complete(handler.aiocall('a', 'a'))
    loop.run_until_complete(handler.aiocall('a', 'b'))
    loop.run_until_complete(handler.aiocall('a.b', 'a.b'))

    print()

    handler = PrefixHandler()
    handler.add('a', callback=[topic_callback1, aiocallback])
    loop.run_until_complete(handler.aiocall('a', 'a'))
    loop.run_until_complete(handler.aiocall('b', 'b'))
    loop.run_until_complete(handler.aiocall('a.b', 'a.b'))
    print()
