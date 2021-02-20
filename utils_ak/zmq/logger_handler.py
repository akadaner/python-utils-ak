import logging

# todo: migrate to loguru


class ZMQLogHandler(logging.Handler):
    def __init__(self, publisher, root_topic=None):
        logging.Handler.__init__(self)
        self.publisher = publisher
        self.root_topic = root_topic

    def emit(self, record):
        topic_list = [record.levelname]
        if self.root_topic:
            topic_list = [self.root_topic] + topic_list
        result_topic = ".".join(topic_list)
        result_msg = self.format(record)
        self.publisher.publish(result_topic, result_msg)
