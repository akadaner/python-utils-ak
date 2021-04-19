import pandas as pd
import os
from utils_ak.pandas import pd_write, pd_read
from utils_ak.os import makedirs, list_files, remove_path
from utils_ak.time import cast_dt, cast_str, cast_datetime_series
from utils_ak.pandas import merge as _merge


class PandasSplitCombineETL:
    def __init__(self, path, key_func=None, prefix="", extension=".csv", merge_by=None):
        self.path = path
        self.extension = extension
        makedirs(path)
        self.key_func = key_func
        self.merge_by = merge_by
        self.prefix = prefix

    def _split(self, combined):
        assert self.key_func, "Key func not defined"
        df = combined
        df["_key"] = self.key_func(df)
        df.columns = [str(c) for c in df.columns]

        if not df.index.name:
            df.index.name = "index"

        df = df.reset_index()
        for key, split in df.groupby("_key"):
            yield key, split.drop(["_key"], axis=1)

    def _fn(self, key):
        values = [self.prefix, key]
        values = [v for v in values if v]
        return os.path.join(self.path, "-".join(values) + self.extension)

    def _load(self, key, split, merge=True):
        fn = self._fn(key)
        if os.path.exists(fn) and merge:
            current_df = self._extract(key)
            split = _merge([current_df, split], by=self.merge_by, sort_index=False)
        pd_write(split, fn, index=False)

    def _get_keys(self):
        fns = list_files(self.path, pattern="*" + self.extension, recursive=True)
        keys = [os.path.splitext(os.path.basename(fn))[0] for fn in fns]
        keys = [key if "-" in key else "" for key in keys]
        # remove prefix if needed
        keys = [
            key[len(self.prefix + "-") :] if key.startswith(self.prefix + "-") else key
            for key in keys
        ]
        return keys

    def _extract(self, key):
        df = pd_read(self._fn(key))
        index_column = df.columns[0]
        df[index_column] = cast_datetime_series(df[index_column])
        df.columns = [str(c) for c in df.columns]
        return df

    def _combine(self, splits_dic, sort_index=True):
        """
        :param splits_dic: {key: split}
        """
        kvs = list(sorted(list(splits_dic.items()), key=lambda kv: kv[0]))
        dfs = [kv[1] for kv in kvs]
        df = pd.concat(dfs, axis=0)
        index_column = df.columns[0]
        df = df.set_index(index_column)
        if sort_index:
            df = df.sort_index()
        return df

    def split_and_load(self, combined, merge=True):
        if not merge:
            remove_path(self.path)
            makedirs(self.path)
        for key, split in self._split(combined):
            self._load(key, split, merge=merge)

    def extract_and_combine(self, sort_index=True):
        keys = self._get_keys()
        splits_dic = {key: self._extract(key) for key in keys}
        return self._combine(splits_dic, sort_index=sort_index)

    def remove(self):
        remove_path(self.path)


def write_pandas_granular(
    df, path, key_func=None, prefix="", extension=".csv", merge_by=None
):
    return PandasSplitCombineETL(
        path, key_func, prefix, extension, merge_by
    ).split_and_load(df)


def read_pandas_granular(
    path, key_func=None, prefix="", extension=".csv", merge_by=None
):
    return PandasSplitCombineETL(
        path, key_func, prefix, extension, merge_by
    ).extract_and_combine()


def test_pandas_split_combine_etl():
    import time
    from utils_ak.time import cast_dt
    from utils_ak.os import remove_path

    df = pd.DataFrame(
        list(range(100)),
        index=pd.date_range(cast_dt("2020.01.01"), periods=100, freq="1d"),
    )

    etl = PandasSplitCombineETL(
        path="test-data/",
        extension=".parquet",
        key_func=lambda df: pd.Series(df.index, index=df.index).apply(
            lambda dt: cast_str(dt, "%Y%m")
        ),
    )

    etl.split_and_load(df)

    print(etl.extract_and_combine())

    remove_path("test-data/")


if __name__ == "__main__":
    test_pandas_split_combine_etl()
