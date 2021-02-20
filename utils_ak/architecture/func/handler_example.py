from utils_ak.architecture import TopicHandler, PrefixHandler, Handler

if __name__ == "__main__":

    def callback1(msg):
        print("callback1", msg)
        return 1

    def callback2(msg):
        print("callback2", msg)
        return 2

    def filter(msg):
        print("filter", msg)
        return "bar1" in msg

    def formatter(msg):
        return (msg.split(".")[0],), {}

    handler = Handler(callback=callback1)
    print("Test 1")
    print(handler("foo.bar1"))
    print(handler("foo.bar2"))
    print()

    handler = Handler(callback=[callback1, callback2], filter=filter)
    print("Test 2")
    print(handler("foo.bar1"))
    print(handler("foo.bar2"))
    print()

    handler = Handler(callback=[callback1, callback2])
    handler.add_filter(filter, rule=lambda b: not b)
    print("Test 3")
    print(handler("foo.bar1"))
    print(handler("foo.bar2"))
    print()

    handler = Handler(
        callback=[callback1, callback2], filter=filter, formatter=formatter
    )
    print("Test 4")
    print(handler("foo.bar1"))
    print(handler("foo.bar2"))
    print()

    def topic_callback1(topic, msg):
        print("callback1", topic, msg)
        return 1

    print("Test 5. Topic Handler")
    handler = TopicHandler()
    handler.add("a.b", callback=topic_callback1)
    handler("a", "")
    handler("b", "")
    handler("a.b", "")
    print()

    print("Test 6. Prefix Handler")
    handler = PrefixHandler()
    handler.add("a", callback=topic_callback1)
    handler("a", "")
    handler("b", "")
    handler("a.b", "")
    print()

    # aio stuff
    import asyncio

    async def aiocallback(topic="", msg=""):
        await asyncio.sleep(0.5)
        print("aiocallback", topic, msg)
        return 3

    loop = asyncio.get_event_loop()

    print("Test 7. AIO 1")
    handler = Handler(callback=[callback1, callback2, aiocallback], filter=filter)
    loop.run_until_complete(handler.aiocall("foo.bar1"))
    loop.run_until_complete(handler.aiocall("foo.bar2"))
    print()

    print("Test 7. AIO Topic Handler")
    handler = TopicHandler()
    handler.add("a.b", callback=aiocallback)
    loop.run_until_complete(handler.aiocall("a", ""))
    loop.run_until_complete(handler.aiocall("a", ""))
    loop.run_until_complete(handler.aiocall("a.b", ""))

    print()

    print("Test 7. AIO Prefix Handler")
    handler = PrefixHandler()
    handler.add("a", callback=[topic_callback1, aiocallback])
    loop.run_until_complete(handler.aiocall("a", ""))
    loop.run_until_complete(handler.aiocall("b", ""))
    loop.run_until_complete(handler.aiocall("a.b", ""))
    print()
