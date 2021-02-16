import tqdm
import os
import fire
import glob


from datetime import datetime, timedelta
from utils_ak.time import *
from utils_ak.pandas import PandasSplitCombineETL
from utils_ak import tqdm
from clickhouse_driver import Client
from utils_ak.state.provider import PickleDBStateProvider
from utils_ak.hash import *


class QuarterlyTableUpdater:
    def __init__(self, input_root, output_root, table):
        self.input_root = input_root
        self.output_root = output_root
        self.table = table
        self.state_provider = PickleDBStateProvider(os.path.join(output_root, table, f'{table}.pickle'))

        self.etl = PandasSplitCombineETL(path=os.path.join(input_root, table),
                                         extension='.parquet',
                                         key_func=lambda df: table,
                                         merge_by=['startRange', 'ticker'])

    def update(self):
        df = self.etl.extract_and_combine()
        beg = df.index[0].to_pydatetime()
        end = round_datetime(datetime.now(), '1d', rounding='floor')

        state = self.state_provider.get_state()

        print('Current state', state)
        cur_year, cur_q_num = state.get('year'), state.get('q_num')

        for year, q_num, quarter_beg, quarter_end in list(iter_quarters(beg, end)):
            print(year, q_num, quarter_beg, quarter_end)
            if cur_year:
                # skip if already processed
                if (year, q_num) < (cur_year, cur_q_num):
                    continue

            if quarter_end == end:
                # add one month for the last file
                quarter_beg = add_months(quarter_beg, -1)
            quarter_df = df[quarter_beg:quarter_end]

            uid = safe_filename_hash(f'{self.table}-{year}-{q_num}')
            base_fn = f'{self.table}_{year}{q_num}'

            fn = os.path.join(self.output_root, self.table, f'{base_fn}_{uid}.csv.zip')
            quarter_df.to_csv(fn, index=False, compression='zip')

        sample_fn = os.path.join(self.output_root, self.table, f'{base_fn}_sample.csv')
        if not os.path.exists(sample_fn):
            df.iloc[:20].to_csv(sample_fn, index=False)

        new_state = {'year': year, 'q_num': q_num}
        self.state_provider.set_state(new_state)


class QuarterlyUpdater:
    def __init__(self, input_root, output_root):
        self.input_root = input_root
        self.output_root = output_root

    def _list_tables(self):
        dirs = glob.glob(os.path.join(self.input_root, '*'))
        tables = [os.path.basename(dir) for dir in dirs]
        return tables

    def update(self):
        tables = self._list_tables()

        for table in tables:
            updater = QuarterlyTableUpdater(self.input_root, self.output_root, table)
            updater.update()


def test_clickhouse_to_fs_updater():
    updater = QuarterlyTableUpdater(input_root=r'C:\Users\Mi\Desktop\master\code\git\python-utils-ak\utils_ak\clickhouse\data',
                                    output_root=r'C:\Users\Mi\Desktop\master\code\git\python-utils-ak\utils_ak\clickhouse\quarterly_data',
                                    table='binance_timebars_600')
    updater.update()


def main(input_root='data', output_root='quarterly_data'):
    QuarterlyUpdater(input_root, output_root).update()


if __name__ == '__main__':
    # test_clickhouse_to_fs_updater()
    fire.Fire(main)