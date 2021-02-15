import pandas as pd
import os
from utils_ak.pandas import pd_write, pd_read
from utils_ak.data_pipelines.etl.SplitCombineETL import SplitCombineETL
from utils_ak.os import makedirs, list_files


class PandasSplitCombineETL(SplitCombineETL):
    def __init__(self, path, extension='.csv'):
        self.path = path
        self.extension = extension
        makedirs(path)

    def _calc_key_pattern(self, df):
        raise NotImplementedError

    def split(self, combined):
        df = combined
        df['_key'] = self._calc_key_pattern(df)
        df.columns = [str(c) for c in df.columns]
        df.index.name = 'index'
        df = df.reset_index()
        for key, split in df.groupby('_key'):
            yield key, split.drop(['_key'], axis=1)

    def _fn(self, key):
        return os.path.join(self.path, key + self.extension)

    def load(self, key, split):
        fn = self._fn(key)
        if os.path.exists(fn):
            current_df = self.extract(key)
            split = pd.concat([current_df, split], axis=0)
        pd_write(split, fn, index=False)

    def get_keys(self, **query):
        fns = list_files(self.path, recursive=True)
        keys = [os.path.splitext(os.path.basename(fn))[0] for fn in fns]
        return keys

    def extract(self, key):
        df = pd_read(self._fn(key))
        df['index'] = cast_datetime_series(df['index'])
        df.columns = [str(c) for c in df.columns]
        return df

    def combine(self, splits_dic):
        """
        :param splits_dic: {key: split}
        """
        dfs = list(splits_dic.values())
        df = pd.concat(dfs, axis=0)
        df = df.set_index('index')
        df = df.sort_index()
        return df


class PandasGranularSplitCombineETL(PandasSplitCombineETL):
    def __init__(self, key_pattern):
        super().__init__('data/')
        self.key_pattern = key_pattern

    def _calc_key_pattern(self, df):
        return pd.Series(df.index, index=df.index).apply(lambda dt: cast_str(dt, self.key_pattern))


if __name__ == '__main__':
    from utils_ak.interactive_imports import *
    df = pd.DataFrame(list(range(100)), index=pd.date_range(cast_dt('2020.01.01'), periods=100, freq='1d'))

    etl = PandasGranularSplitCombineETL('%Y%m')
    print(etl._calc_key_pattern(df))
    etl.split_and_load(df)
    print(etl.extract_and_combine())
