# combined -> split -> load
# get_keys -> extract -> combine

class SplitCombineETL:
    def split(self, combined):
        raise NotImplementedError

    def load(self, key, split):
        raise NotImplementedError

    def get_keys(self, **query):
        raise NotImplementedError

    def extract(self, key):
        raise NotImplementedError

    def combine(self, splits_dic):
        """
        :param splits_dic: {key: split}
        """
        raise NotImplementedError

    def split_and_load(self, combined):
        for key, split in self.split(combined):
            self.load(key, split)

    def extract_and_combine(self, **query):
        keys = self.get_keys(**query)
        splits_dic = {key: self.extract(key) for key in keys}
        return self.combine(splits_dic)