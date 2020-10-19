""" PrefixHandler inherits a TopicHandler with callbacks triggered by prefix rule. """
from utils_ak.str import cast_unicode
from .topic_handler import TopicHandler


class PrefixHandler(TopicHandler):
    def __call__(self, topic, *args, **kwargs):
        # topic are in unicode
        topic = cast_unicode(topic)
        args = [topic] + list(args)
        res = []
        for _topic, handler in self.handlers.items():
            if topic.startswith(_topic):
                res.append(handler(*args, **kwargs))
        return res

    def call(self, topic, *args, **kwargs):
        return self.__call__(topic, *args, **kwargs)

    async def aiocall(self, topic, *args, **kwargs):
        # topic are in unicode
        topic = cast_unicode(topic)
        args = [topic] + list(args)
        res = []
        for _topic, handler in self.handlers.items():
            if topic.startswith(_topic):
                res.append(await handler.aiocall(*args, **kwargs))
        return res

