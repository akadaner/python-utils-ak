from .js import dumps, loads
from .serializer import Serializer


class JsonSerializer(Serializer):
    def __init__(self, encoding='utf-8', indent=None):
        self.encoding = encoding
        self.indent = indent

    def encode(self, obj):
        s = dumps(obj, indent=self.indent)
        if self.encoding:
            return s.encode(self.encoding)
        else:
            return s

    def decode(self, s):
        return loads(s)
