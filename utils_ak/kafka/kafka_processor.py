# online/offline
# cache for state
# state - подается на вход?

# один processor на один топик? Да, но client снаружи - слушает сообщения
# Как безопасно хранить state? В чем проблема хранить в файле? Слишком часто обновлять и перезаписывать файл.

from .kafka_client import KafkaClient


class KafkaProcessor:
    def __init__(self, topic, bootstrap_servers='localhost:9092'):
        pass
