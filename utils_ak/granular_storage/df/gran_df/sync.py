from utils_ak.time.dt import *
from utils_ak.pd_tools import find_row
from utils_ak.os_tools import remove

from utils_ak.tqdm_tools import tqdm_ak


class GranularSync(object):
    def __init__(self, gs_from, gs_to):
        self.gs_from = gs_from
        self.gs_to = gs_to

    def sync(self, path_from, path_to, flag_migration=False):
        if not self.gs_from.exists(path_from):
            raise Exception(f'Path not exists {path_from} in gran_from')

        gran_from = self.gs_from.get_granular(path_from)
        if not self.gs_to.exists(path_to):
            # inherit format from gran_from
            gran_to = self.gs_to.get_granular(path_to, level=gran_from.level, pattern=gran_from.pattern)
        else:
            gran_to = self.gs_to.get_granular(path_to)

        # sync gran1 into gran2
        struct_from = gran_from.get_struct_df()
        struct_to = gran_to.get_struct_df()

        last_struct_to_df = struct_to.groupby('key').agg('last')
        last_rows = last_struct_to_df.to_dict('index')

        for key in tqdm_ak(struct_from['key'].unique()):
            if key not in last_rows:
                # key is not present in gran_to
                df = gran_from.read(key)
            else:
                last_df_to = gran_to.read(key, beg=last_rows[key]['ts'])
                last_row = last_df_to.iloc[[-1]]

                df = gran_from.read(key, beg=last_row.index[0])

                last_row_idx = find_row(df, last_row)

                if last_row_idx is not None:
                    df = df.iloc[last_row_idx + 1:]

                if len(df) == 0:
                    continue
            gran_to.append(df, key)

        # see Streamer
        if flag_migration:
            gran_to.upd_metadata({'migration_complete': True})


if __name__ == '__main__':
    from utils_ak.granular_storage.df.gran_df import GranularFeatherStorage

    gs1 = GranularFeatherStorage('gs_root1')
    gs2 = GranularFeatherStorage('gs_root2')

    index = [cast_datetime('2018.05.01') + timedelta(hours=6) * 0 for i in range(6)]
    # df1 = pd.DataFrame(np.random.randn(6, 1), index=index, columns=['0'])
    df1 = pd.DataFrame([np.random.randn(1, 1)[0]] * 5 + [[2]], index=index, columns=['0'])

    gran1 = gs1.get_granular('gran1', level=0, mode='w')
    gran2 = gs2.get_granular('gran2', level=4, mode='w')

    gran1.append(df1, 'foo1.feather')
    gran2.append(df1.iloc[:-1], 'foo1.feather')

    sync = GranularSync(gs1, gs2)
    sync.sync('gran1', 'gran2')
    gran2 = gs2['gran2']

    print((gran1['foo1.feather'].equals(gran2['foo1.feather'])))

    # clean test granular_storage
    remove('gs_root1/')
    remove('gs_root2/')
