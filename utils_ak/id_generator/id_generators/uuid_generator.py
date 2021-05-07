from utils_ak.id_generator.id_generator import IDGenerator
import uuid


class UUIDGenerator(IDGenerator):
    def gen_id(self):
        return str(uuid.uuid4())
