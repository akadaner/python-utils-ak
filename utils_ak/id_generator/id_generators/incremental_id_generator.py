from utils_ak.id_generator.id_generator import IDGenerator
import uuid


class IncrementalIDGenerator(IDGenerator):
    def __init__(self, start_from=0):
        self.cur_id = start_from

    def gen_id(self):
        res = self.cur_id
        self.cur_id += 1
        return res
