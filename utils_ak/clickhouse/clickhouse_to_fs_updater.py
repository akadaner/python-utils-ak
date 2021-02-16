import tqdm
import os
import fire

from datetime import datetime, timedelta
from utils_ak.time import *
from utils_ak.pandas import PandasSplitCombineETL
from utils_ak import tqdm
from clickhouse_driver import Client
from utils_ak.state.provider import PickleDBStateProvider

clickhouse_client = Client(CLICKHOUSE_URL, database=CLICKHOUSE_DB)


class Clickhouse2FsDatasetUpdater:
    def __init__(self, root, clickhouse_dataset, start_id=None, freq='1d'):
        self.root = root
        self.start_id = start_id

        self.state_provider = PickleDBStateProvider(os.path.join(root, clickhouse_dataset, f'{clickhouse_dataset}.pickle'))
        self.clickhouse_dataset = clickhouse_dataset
        self.freq = freq

        # todo: choose granularity automatically
        # todo: other merge_by tickers?
        self.etl = PandasSplitCombineETL(path=os.path.join(root, clickhouse_dataset),
                                         extension='.parquet',
                                         key_func=lambda df: clickhouse_dataset,
                                         merge_by=['startRange', 'ticker'])

    def _init(self):
        resp = clickhouse_client.execute(f'DESCRIBE TABLE {self.clickhouse_dataset}')  # [('sepal_length', 'Decimal(9, 2)', '', '', '', '', ''),...
        self.columns = [v[0] for v in resp]

    def _generate_jobs(self, last_id):
        last_id = cast_datetime(last_id)
        beg = last_id
        end = round_datetime(datetime.now(), self.freq, rounding='floor')
        ranges = iter_range(beg, end, cast_timedelta('1d'))
        return [{'clickhouse_dataset': self.clickhouse_dataset, 'beg': beg, 'end': end} for beg, end in ranges]

    def _combine(self, outputs):
        outputs = list(sorted(outputs, key=lambda output: output['job']['beg']))
        new_state = {'last_id': cast_str(max(output['job']['beg'] for output in outputs))}
        combined = sum([output['response'] for output in outputs], [])
        return new_state, combined

    def _apply(self, update):
        df = pd.DataFrame(update, columns=self.columns).set_index('startRange')
        self.etl.split_and_load(df)

    def _init_start_id(self):
        first_id = clickhouse_client.execute(f'SELECT startRange FROM {self.clickhouse_dataset} LIMIT 1')[0][0]
        self.start_id = round_datetime(first_id, timedelta(days=1), rounding='floor')

    def _process(self, job):
        query = "SELECT * FROM {clickhouse_dataset} WHERE (startRange >= '{beg}' AND startRange < '{end}')"
        return {'job': job, 'response': clickhouse_client.execute(query.format(**job))}

    def update(self):
        self._init()
        state = self.state_provider.get_state()
        if not state.get('last_id'):
            self._init_start_id()
        last_id = state.get('last_id', self.start_id)
        jobs = self._generate_jobs(last_id)
        outputs = [self._process(job) for job in tqdm(jobs, desc=jobs[0]['clickhouse_dataset'])]
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
    updater = Clickhouse2FsDatasetUpdater(root='data', clickhouse_dataset='binance_timebars_600')
    updater.update()
    print(updater.etl.extract_and_combine())


def main(root='data'):
    Clickhouse2FsUpdater(root).update()


if __name__ == '__main__':
    # test_clickhouse_to_fs_updater()
    fire.Fire(main)