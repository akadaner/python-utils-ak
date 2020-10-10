from utils.granular_storage.storage.fs_storage import GranularTable, Storage, DB

from utils.granular_storage.table.timeseries.fs_json import FsJsonTimeSeriesTable
from utils.granular_storage.table.timeseries.fs_msgpack import FsMsgPackTimeSeriesTable


class JsonGranularTable(GranularTable):
    @property
    def table_cls(self):
        return FsJsonTimeSeriesTable


class MsgPackGranularTable(GranularTable):
    @property
    def table_cls(self):
        return FsMsgPackTimeSeriesTable


class JsonGranularStorage(Storage):
    @property
    def table_cls(self):
        return JsonGranularTable

    @property
    def db_cls(self):
        return DB

    @property
    def ext(self):
        return '.txt'


class MsgPackGranularStorage(Storage):
    @property
    def table_cls(self):
        return MsgPackGranularTable

    @property
    def db_cls(self):
        return DB

    @property
    def ext(self):
        return '.msg'


if __name__ == '__main__':
    from datetime import datetime, timedelta

    storage = JsonGranularStorage('/tmp/')
    db = storage.get_db('foo_db')
    table = db.get_table('foo_table', pattern='%Y%m%d_{base}')

    dt = datetime.now()
    values = [[dt.timestamp(), 'key', {'a': i}] for i in range(3)]
    dt += timedelta(days=1)
    values += [[dt.timestamp(), 'key', {'b': i}] for i in range(3)]
    dt += timedelta(days=1)
    values += [[dt.timestamp(), 'key', {'c': i}] for i in range(3)]

    for i in range(3):
        table.store(datetime.now().timestamp(), 'key', {'a': i})

    table.store_many(values, safe=False, overwrite=True)
