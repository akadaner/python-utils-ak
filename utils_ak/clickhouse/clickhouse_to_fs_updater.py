import tqdm

from datetime import datetime, timedelta
from utils_ak.time import *
from utils_ak.pandas import PandasSplitCombineETL
from clickhouse_driver import Client



clickhouse_client = Client(CLICKHOUSE_URL, database=CLICKHOUSE_DB)


class DefaultJobRunner:
    def run_jobs(self, worker, jobs):
        return [worker(job) for job in tqdm.tqdm(jobs)]


class Clickhouse2FsDatasetUpdater:
    def __init__(self, clickhouse_dataset, state_provider, start_id, job_runner=None, freq='1d'):
        self.state_provider = state_provider
        self.job_runner = job_runner or DefaultJobRunner()
        self.start_id = start_id

        self.clickhouse_dataset = clickhouse_dataset
        self.freq = freq

        # todo: make properly
        self.etl = PandasSplitCombineETL(path='data/',
                                         extension='.csv',
                                         key_func=lambda df: pd.Series(df.index, index=df.index).apply(lambda dt: cast_str(dt, '%Y%m')),
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
        # todo: del
        df = df[['ticker', 'pOpen']]
        self.etl.split_and_load(df)

    def update(self, worker):
        self._init()
        state = self.state_provider.get_state()
        last_id = state.get('last_id', self.start_id)
        jobs = self._generate_jobs(last_id)
        outputs = self.job_runner.run_jobs(worker, jobs)
        new_state, update = self._combine(outputs)
        self._apply(update)
        self.state_provider.set_state(new_state)


def worker(job):
    query = "SELECT * FROM {clickhouse_dataset} WHERE (startRange >= '{beg}' AND startRange < '{end}')"
    return {'job': job, 'response': clickhouse_client.execute(query.format(**job))}


def test_clickhouse_to_fs_updater():
    from utils_ak.state.provider import PickleDBStateProvider
    updater = Clickhouse2FsDatasetUpdater(clickhouse_dataset='binance_timebars_600',
                                          state_provider=PickleDBStateProvider('clickhouse_dataset2.pickle'),
                                          start_id=cast_dt('2020.07.01'))
    updater.update(worker)
    print(updater.etl.extract_and_combine())


if __name__ == '__main__':
    test_clickhouse_to_fs_updater()