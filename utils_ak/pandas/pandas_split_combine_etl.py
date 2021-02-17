import pandas as pd
import os
from utils_ak.pandas import pd_write, pd_read
from utils_ak.os import makedirs, list_files, remove_path
from utils_ak.time import cast_dt, cast_str, cast_datetime_series
from utils_ak.pandas import merge


class PandasSplitCombineETL:
    def __init__(self, path, key_func, prefix='', extension='.csv', merge_by=None):
        self.path = path
        self.extension = extension
        makedirs(path)
        self.key_func = key_func
        self.merge_by = merge_by
        self.prefix = prefix

    def _split(self, combined):
        df = combined
        df['_key'] = self.key_func(df)
        df.columns = [str(c) for c in df.columns]
        if not df.index.name:
            df.index.name = 'index'
        df = df.reset_index()
        for key, split in df.groupby('_key'):
            yield key, split.drop(['_key'], axis=1)

    def _fn(self, key):
        values = [self.prefix, key]
        values = [v for v in values if v]
        return os.path.join(self.path, '-'.join(values) + self.extension)

    def _load(self, key, split):
        fn = self._fn(key)
        if os.path.exists(fn):
            current_df = self._extract(key)
            if self.merge_by:
                split = merge([current_df, split], by=self.merge_by)
            else:
                split = pd.concat([current_df, split], axis=0)
        pd_write(split, fn, index=False)

    def _get_keys(self):
        fns = list_files(self.path, pattern='*' + self.extension, recursive=True)
        keys = [os.path.splitext(os.path.basename(fn))[0] for fn in fns]
        # remove prefix if needed
        keys = [key[len(self.prefix + '-'):] if key.startswith(self.prefix + '-') else key for key in keys]
        return keys

    def _extract(self, key):
        df = pd_read(self._fn(key))
        index_column = df.columns[0]
        df[index_column] = cast_datetime_series(df[index_column])
        df.columns = [str(c) for c in df.columns]
        return df

    def _combine(self, splits_dic):
        """
        :param splits_dic: {key: split}
        """
        dfs = list(splits_dic.values())
        df = pd.concat(dfs, axis=0)
        index_column = df.columns[0]
        df = df.set_index(index_column)
        df = df.sort_index()
        return df

    def split_and_load(self, combined):
        for key, split in self._split(combined):
            self._load(key, split)

    def extract_and_combine(self):
        keys = self._get_keys()
        splits_dic = {key: self._extract(key) for key in keys}
        return self._combine(splits_dic)


def test_pandas_split_combine_etl():
    from utils_ak import cast_dt, remove_path
    df = pd.DataFrame(list(range(100)), index=pd.date_range(cast_dt('2020.01.01'), periods=100, freq='1d'))

    etl = PandasSplitCombineETL(path='data/',
                                extension='.parquet',
                                key_func=lambda df: pd.Series(df.index, index=df.index).apply(lambda dt: cast_str(dt, '%Y%m')))
    etl.split_and_load(df)
    print(etl.extract_and_combine())

    for key in etl._get_keys():
        remove_path(etl._fn(key))


if __name__ == '__main__':
    test_pandas_split_combine_etl()