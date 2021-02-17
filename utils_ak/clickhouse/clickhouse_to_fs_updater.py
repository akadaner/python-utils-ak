import tqdm
import os
import fire

from datetime import datetime, timedelta
from utils_ak.time import *
from utils_ak.pandas import PandasSplitCombineETL
from utils_ak.builtin import *
from utils_ak import tqdm
from utils_ak.loguru import configure_loguru_stdout

from clickhouse_driver import Client
from utils_ak.state.provider import PickleDBStateProvider
from loguru import logger


clickhouse_client = Client(CLICKHOUSE_URL, database=CLICKHOUSE_DB)


class Clickhouse2FsDatasetUpdater:
    def __init__(self, root, clickhouse_dataset, freq='1d'):
        self.root = root
        self.state_provider = PickleDBStateProvider(os.path.join(root, clickhouse_dataset, f'{clickhouse_dataset}.pickle'))
        self.clickhouse_dataset = clickhouse_dataset
        self.freq = freq

        self.last_id = None
        self.granularity_key = None
        self.etl = None

    def _init(self):
        resp = clickhouse_client.execute(f'DESCRIBE TABLE {self.clickhouse_dataset}')  # [('sepal_length', 'Decimal(9, 2)', '', '', '', '', ''),...
        self.columns = [v[0] for v in resp]

    def _get_new_jobs(self):
        last_id = cast_datetime(self.last_id)

        beg = last_id
        end = round_datetime(datetime.now(), self.freq, rounding='floor')
        ranges = iter_range(beg, end, cast_timedelta('1d'))
        return [{'clickhouse_dataset': self.clickhouse_dataset, 'beg': beg, 'end': end} for beg, end in ranges]

    def _combine(self, outputs):
        outputs = list(sorted(outputs, key=lambda output: output['job']['beg']))

        new_state = {'last_id': cast_str(max(output['job']['beg'] for output in outputs)),
                     'granularity_key': self.granularity_key}

        combined = sum([output['response'] for output in outputs], [])
        return new_state, combined

    def _apply(self, update):
        df = pd.DataFrame(update, columns=self.columns).set_index('startRange')
        self.etl.split_and_load(df)

    def _get_etl(self):
        if not self.granularity_key:
            key_func = lambda df: ''
        else:
            key_func = lambda df: pd.Series(df.index, index=df.index).apply(lambda dt: cast_str(dt, self.granularity_key))

        return PandasSplitCombineETL(path=os.path.join(self.root, self.clickhouse_dataset),
                                         extension='.parquet',
                                         key_func=key_func,
                                     merge_by=['startRange', 'ticker'],
                                     prefix=self.clickhouse_dataset)

    def _get_last_id(self):
        if 'last_id' in self.state_provider.get_state():
            return self.state_provider.get_state()['last_id']
        first_id = clickhouse_client.execute(f"SELECT min(startRange) FROM binance_timebars_1800")[0][0]
        return round_datetime(first_id, timedelta(days=1), rounding='floor')

    def _get_granularity_key(self):
        if 'granularity_key' in self.state_provider.get_state():
            return self.state_provider.get_state()['granularity_key']
        beg = clickhouse_client.execute(f"SELECT min(startRange) FROM binance_timebars_1800")[0][0]
        end = clickhouse_client.execute(f"SELECT max(startRange) FROM binance_timebars_1800")[0][0]

        total_days = (end - beg).total_seconds() / (24 * 60 * 60)

        query = f"""SELECT sum(bytes) as size,
            min(min_date) as min_date,
            max(max_date) as max_date
            FROM system.parts
            WHERE (active AND table == '{self.clickhouse_dataset}')"""
        size_bytes = clickhouse_client.execute(query)[0][0]
        size_mb = size_bytes / 1024 / 1024

        avg_size_per_day = size_mb / total_days

        granularity_levels = {None: 5 * 365, '%Y': 365, '%Y%m': 30, '%Y%m%d': 1, '%Y%m%d%H': 1 / 24}

        def get_level(granularity_levels, avg_size_per_day):
            for key, days in sorted(granularity_levels.items(), key=lambda kv: kv[1], reverse=True):
                if avg_size_per_day * days / 3 < 1000:  # ~1GB in parquet file
                    return key
            return key

        return get_level(granularity_levels, avg_size_per_day)

    def _process(self, job):
        query = "SELECT * FROM {clickhouse_dataset} WHERE (startRange >= '{beg}' AND startRange < '{end}')"
        return {'job': job, 'response': clickhouse_client.execute(query.format(**job))}

    def update(self):
        self._init()

        self.last_id = self._get_last_id()
        self.granularity_key = self._get_granularity_key()
        logger.info('Starting', last_id=self.last_id, granularity_key=self.granularity_key)
        self.etl = self._get_etl()

        jobs = self._get_new_jobs()
        chunks = list(crop_to_chunks(jobs, 100))

        for i, chunk in enumerate(chunks):
            outputs = [self._process(job) for job in tqdm(chunk, desc='{} {}/{}'.format(jobs[0]['clickhouse_dataset'], i + 1, len(chunks)))]
            new_state, update = self._combine(outputs)
            self._apply(update)
            self.state_provider.set_state(new_state)


class Clickhouse2FsUpdater:
    def __init__(self, root):
        self.root = root

    def _list_tables(self):
        res = clickhouse_client.execute('SHOW TABLES')
        return [x[0] for x in res]

    def update(self):
        tables = self._list_tables()
        tables = [table for table in tables if not table.endswith('_streaming') and not table.endswith('_test')]

        for table in tables:
            updater = Clickhouse2FsDatasetUpdater(root=self.root, clickhouse_dataset=table)
            updater.update()


def test_clickhouse_to_fs_updater():
    configure_loguru_stdout()
    updater = Clickhouse2FsDatasetUpdater(root='data', clickhouse_dataset='binance_timebars_1800')
    updater.update()
    print(updater.etl.extract_and_combine())


def main(root='data'):
    configure_loguru_stdout()
    Clickhouse2FsUpdater(root).update()


if __name__ == '__main__':
    test_clickhouse_to_fs_updater()
    # fire.Fire(main)