""" TopicHandler is a builtin of handlers, bound to certain topics as callbacks. """
from .handler import Handler

# one topic for one handler
class TopicHandler(object):
    """ Run handlers on events. Each event has topic as it's id, which is passed as argument to handler functions. """

    def __init__(self, topic_formatter=None):
        # {topic: handler}
        self.handlers = {}
        self.topic_formatter = topic_formatter

    def add(self, topic, callback=None, formatter=None, filter=None, reducer=None):
        handler = Handler(reducer=reducer)
        handler.add(callback=callback, filter=filter, formatter=formatter)
        self.handlers.setdefault(topic, Handler()).add(callback=handler)

    def has_coroutine(self, topic):
        return self.handlers[topic].has_coroutine()

    def set_topic_formatter(self, formatter):
        self.topic_formatter = formatter

    def __call__(self, topic, *args, **kwargs):
        args = [topic] + list(args)
        if self.topic_formatter:
            topic = self.topic_formatter(topic)

        if topic in self.handlers:
            return self.handlers[topic](*args, **kwargs)

    def call(self, topic, *args, **kwargs):
        return self.__call__(topic, *args, **kwargs)

    async def aiocall(self, topic, *args, **kwargs):
        args = [topic] + list(args)
        if self.topic_formatter:
            topic = self.topic_formatter(topic)

        if topic in self.handlers:
            return await self.handlers[topic].aiocall(*args, **kwargs)

    def has_topic(self, topic):
        return topic in self.handlers

    def __getitem__(self, item):
        return self.handlers[item]
