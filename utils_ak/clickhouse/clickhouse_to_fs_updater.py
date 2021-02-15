from datetime import datetime, timedelta

from utils_ak.time import *
from utils_ak.updater import *
from utils_ak.data_pipelines import PandasGranularSplitCombineETL
from clickhouse_driver import Client


clickhouse_client = Client(CLICKHOUSE_URL, database=CLICKHOUSE_DB)


class Clickhouse2FsDatasetUpdater(Updater):
    def __init__(self, clickhouse_dataset, freq='1d', **kwargs):
        self.clickhouse_dataset = clickhouse_dataset
        self.freq = freq
        # todo: make properly
        self.etl = PandasGranularSplitCombineETL('%Y%m%d', path='data/', extension='.csv')
        super().__init__(**kwargs)

    def init(self):
        resp = clickhouse_client.execute(f'DESCRIBE TABLE {self.clickhouse_dataset}')  # [('sepal_length', 'Decimal(9, 2)', '', '', '', '', ''),...
        self.columns = [v[0] for v in resp]

    def generate_jobs(self, last_id):
        last_id = cast_datetime(last_id)

        beg = last_id
        # end = round_datetime(datetime.now(), self.freq, rounding='floor')
        end = beg + timedelta(days=1)
        ranges = jobs = iter_range(beg, end, cast_timedelta('1d'))
        return [{'clickhouse_dataset': self.clickhouse_dataset, 'beg': beg, 'end': end} for beg, end in ranges]

    def combine(self, outputs):
        outputs = list(sorted(outputs, key=lambda output: output['job']['beg']))
        new_state = {'last_id': cast_str(max(output['job']['beg'] for output in outputs))}
        combined = sum([output['response'] for output in outputs], [])
        return new_state, combined

    def apply(self, update):
        df = pd.DataFrame(update, columns=self.columns).set_index('startRange')
        self.etl.split_and_load(df)


def worker(job):
    query = "SELECT * FROM {clickhouse_dataset} WHERE (startRange >= '{beg}' AND startRange < '{end}')"
    return {'job': job, 'response': clickhouse_client.execute(query.format(**job))}


def test_clickhouse_to_fs_updater():
    from utils_ak.state.provider import PickleDBStateProvider
    updater = Clickhouse2FsDatasetUpdater('binance_timebars_600',
                                          state_provider=PickleDBStateProvider('clickhouse_dataset2.pickle'),
                                          start_id=cast_dt('2020.07.01'))
    updater.update(worker)

if __name__ == '__main__':
    test_clickhouse_to_fs_updater()