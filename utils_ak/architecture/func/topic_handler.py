""" TopicHandler is a builtin of handlers, bound to certain topics as callbacks. """
from .handler import Handler
from utils_ak.builtin import delistify
from sortedcollections import OrderedDict

class TopicHandler(object):
    """ Run handlers on events. Each event has topic as it's id, which is passed as argument to handler functions. """

    def __init__(self, topic_formatter=None, topic_filter=None, reducer=None):
        self.handlers = OrderedDict()  # {topic: handler}
        self.topic_formatter = topic_formatter
        self.topic_filter = topic_filter or (lambda topic, received_ropic: topic == received_ropic)
        self.reducer = reducer or delistify

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

        res = []
        for _topic, handler in self.handlers.items():
            if self.topic_filter(_topic, topic):
                res.append(handler(*args, **kwargs))
        return self.reducer(res)

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


class PrefixHandler(TopicHandler):
    def __init__(self, topic_formatter=None, reducer=None):
        super().__init__(topic_formatter=topic_formatter,
                         topic_filter=lambda topic, received_topic: received_topic.startswith(topic),
                         reducer=reducer)
