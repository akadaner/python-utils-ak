import pandas as pd
import numpy as np

import os
import base64

from PIL import Image
from io import BytesIO

from utils_ak.os import *
from utils_ak.builtin import list_get

pd.set_option("display.max_colwidth", None)


def display_with_image_thumbnails(df, shape=None):
    from IPython.display import HTML, display

    def _get_thumbnail(path, shape=None):
        if shape is None:
            shape = (150, 150)
        i = Image.open(path)
        i.thumbnail(shape, Image.LANCZOS)
        return i

    def _image_base64(im, shape):
        if isinstance(im, str):
            im = _get_thumbnail(im, shape)
        with BytesIO() as buffer:
            im.save(buffer, "jpeg")
            return base64.b64encode(buffer.getvalue()).decode()

    def _image_formatter(im, shape):
        if type(im) != str:
            return str(im)
        possible_path = im
        if not os.path.exists(possible_path):
            return im

        ext = os.path.splitext(possible_path)[-1]

        if ext not in [".png", ".jpg"]:
            return im

        return '<img src="granular_storage:image/jpeg;base64,{}">'.format(
            _image_base64(im, shape)
        )

    columns = df.columns
    formatters = {col: lambda x: _image_formatter(x, shape) for col in columns}
    # displaying PIL.Image objects embedded in dataframe
    display(HTML(df.to_html(formatters=formatters, escape=False)))


def where(cond, x, y):
    """np.where analogue for dataframes with the same signature. Return elements, either from x or y, depending on condition.

    Parameters
    ----------
    cond: boolean condition
    cond_value: value to return when cond is True
    other: value to return when cond is False

    Returns
    -------
    pd.DataFrame
        np.where wrapped pandas dataframe
    """
    if isinstance(x, pd.DataFrame):
        df = x
    elif isinstance(y, pd.DataFrame):
        df = y
    elif isinstance(cond, pd.DataFrame):
        df = cond
    else:
        raise Exception("Either cond, x, y must be a dataframe")
    return pd.DataFrame(np.where(cond, x, y), index=df.index, columns=df.columns)


def series_equal(s1, s2, dtype="float", eps=1e-8):
    if dtype == "float":
        return s1.sub(s2).abs() < eps
    else:
        return s1 == s2


def find_row(df, row):
    """Find last occurence of row in df
    :df: pd.DataFrame
    :row: pd.DataFrame with unit length
    :return: int. Row number
    """
    if len(row) != 1:
        raise Exception("Row should be one line dataframe")

    if not np.all(df.columns == row.columns):
        raise Exception("Dataframe and row have different columns")

    # filter by index
    # time is considered discrete - no problem on rounding here
    df.index.name = "index"
    df = df.reset_index()
    tmp = df[df["index"] == row.index[0]].copy()
    tmp.pop("index")

    if len(tmp) == 0:
        return None

    # filter by values
    for col in tmp.columns:
        # todo: make better
        dtype = "float" if "float" in str(df.dtypes[0]) else None

        row_series = pd.Series(index=tmp[col].index)
        row_series[:] = row[col].iloc[0]

        tmp[col] = series_equal(tmp[col], row_series, dtype=dtype)
    """ 0    False
        1     True
        2    False
        3    False
        dtype: bool"""
    row_equal = tmp.all(axis=1)
    tmp = tmp[row_equal]

    if len(tmp) == 0:
        return None

    return tmp.index[-1]


def merge_by_columns(dfs):
    res_df = dfs[0].copy()
    for df in dfs[1:]:
        for key in df.columns:
            res_df[key] = df[key]
    return res_df


def merge(dfs, by=None, by_index=False, keep="last", sort_index=True):
    """
    :param dfs: list(`pd.DataFrame`)
    :param by: name of column or list of columns names. 'all' for all columns. 'columns' for merge_by_columns method
    :param by_index: include index or not (bool value)
    :param keep: 'last' or 'first'

    :return:
    """
    if not by and not by_index:
        raise TypeError("Either by or index should by non-trivial")

    if by is None:
        by = []
    elif by == "all":
        pass
    elif by == "columns":
        return merge_by_columns(dfs)
    elif isinstance(by, str):
        by = [by]
    elif isinstance(by, list):
        pass
    else:
        raise TypeError("By should be list, str or None")

    df = pd.concat(dfs, axis=0)

    masks = []
    if by == "all":
        masks.append(df)
    elif by:
        masks.append(df[by])

    if by_index:
        masks.append(pd.Series(df.index, index=df.index))
    mask = pd.concat(masks, axis=1)
    df = df[~mask.duplicated(keep=keep)]

    if sort_index:
        df = df.sort_index()
    return df


def find_gaps(index, gap):
    index = pd.Series(index, index=index)
    index_diff = index - index.shift(1)
    index_diff = index_diff[index_diff > gap]
    return index_diff


# todo: search for existing solutions
def pd_read(fn, **kwargs):
    ext = os.path.splitext(fn)[-1]
    if ".zip" in ext:
        ext = os.path.splitext(fn[:-4])[-1]
    return getattr(pd, f"read_{ext[1:]}")(fn, **kwargs)


def pd_write(df, fn, **kwargs):
    ext = os.path.splitext(fn)[-1]
    if ".zip" in ext:
        ext = os.path.splitext(fn[:-4])[-1]
        kwargs["compression"] = "zip"
    kwargs["index"] = False

    tmp_fn = fn + ".tmp"
    res = getattr(df, f"to_{ext[1:]}")(tmp_fn, **kwargs)
    if os.path.exists(fn):
        remove_path(fn)
    rename_path(tmp_fn, fn)
    return res


def mark_consecutive_groups(df, key, groups_key):
    cur_value = None
    cur_i = 0

    values = []
    for i, row in df.iterrows():
        if row[key] != cur_value:
            cur_i += 1
            cur_value = row[key]
        values.append(cur_i)
    df[groups_key] = values


if __name__ == "__main__":
    df1 = pd.DataFrame.from_dict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [1, 1, 1]})
    df1 = df1.set_index("c")
    df2 = pd.DataFrame.from_dict({"a": [3, 4, 5], "b": [7, 8, 9]})

    print(df1)
    print("-----")
    print(df2)

    print("----------")

    dfs = [df1, df2]

    print(merge(dfs, index=False, by="a"))
    print()
    print(merge(dfs, index=True, by="a"))
    print()
    print(merge(dfs, index=True, by=None))
    print()
    print(merge(dfs, index=True, by=None, keep="first"))
    print()
    print(merge(dfs, index=True, by="all", keep="first"))

    try:
        print(merge(dfs, index=False, by=None))
    except Exception as e:
        print(e)
    else:
        raise Exception("Should not happen")
