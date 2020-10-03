""" Storing and retrieving data in the filesystem . """

from utils_ak.data.df.file import *
from utils_ak.data.df.gran_df import GranularMsgPackStorage, GranularParquetStorage

GRAN_LEVEL_ZERO = 0
GRAN_LEVEL_YEAR = 1
GRAN_LEVEL_MONTH = 2
GRAN_LEVEL_DAY = 3
GRAN_LEVEL_HOUR = 4

MainStorage = GranularMsgPackStorage
#
# if __name__ == '__main__':
#     storage = MainStorage('.')
#     gran_df = storage.get_granular('some/path/to/granular', level=GRAN_LEVEL_ZERO)
#     gran_df.append(df, append=False)
