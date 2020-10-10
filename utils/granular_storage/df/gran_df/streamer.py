from utils.granular_storage.df.gran_df.sync import GranularSync
from utils.time.dt import *


class GranularStreamer(object):
    def __init__(self, gs, path, buffer_timeout=24 * 3600, level=3, stream_level=4, init_path=False, migration_expected=True):
        self.gs = gs
        self.path = path
        self.stream_path = path + '_stream'
        self.migration_expected = migration_expected

        self.gran = gs.get_granular(path, level=level)

        # todo: make level properly
        self.level = level
        self.stream_level = stream_level
        self.init_stream_gran(mode='a')

        self.last_sync = datetime.now()
        self.buffer_timeout = buffer_timeout

        if init_path and not self.gs.exists(path):
            self.gs.get_granular(path, level=self.level)

    def init_stream_gran(self, mode):
        self.gran_stream = self.gs.get_granular(self.stream_path, level=self.stream_level, mode=mode)

    def flush(self):
        self.gran_stream.flush()

    def append_stream(self, df, key):
        self.gran_stream.append_stream(df, key)
        if cast_sec((datetime.now() - self.last_sync)) > self.buffer_timeout:
            self.last_sync = datetime.now()
            self.sync_with_base()

    def sync_with_base(self):
        print('Sync with base')
        # base granular not exists yet. Waiting for migration
        if not self.gs.exists(self.path):
            print('path not exists')
            return

        gran_base = self.gs[self.path]
        # waiting for migration to be complete before syncing
        if self.migration_expected and not gran_base.metadata.get('migration_complete'):
            print('Migration not complete yet')
            return

        print('Writing!')
        # handle opened files and current buffer
        self.gran_stream.flush()
        self.gran_stream.close()

        sync = GranularSync(self.gs, self.gs)
        sync.sync(self.stream_path, self.path)

        # initialize new streamer with write mode - to remove current stream granular_storage
        self.init_stream_gran(mode='w')


def ex_stream():
    print('-' * 100)
    print('Stream test')
    print('-' * 100)
    import time
    from utils.granular_storage.df.gran_df import GranularMsgPackStorage as Storage
    from utils.os_tools import remove
    remove('granular_storage/gs_root/')
    gs = Storage('granular_storage/gs_root')

    index = [cast_datetime('2018.05.01') + timedelta(hours=6) * i for i in range(3)]
    df = pd.DataFrame(np.random.randn(3, 1), index=index, columns=['0'])

    streamer = GranularStreamer(gs, 'sample_path', buffer_timeout=3, init_path=True, migration_expected=False)
    streamer.append_stream(df.iloc[:1], 'sample_table')

    def print_table():
        try:
            print(gs['sample_path']['sample_table'])
        except:
            print('No table yet')

    time.sleep(4)
    streamer.append_stream(df.iloc[1:2], 'sample_table')
    print_table()
    time.sleep(1)
    streamer.append_stream(df.iloc[2:3], 'sample_table')
    print_table()
    streamer.flush()
    print_table()
    streamer.sync_with_base()
    print_table()


def ex_migration():
    print('-' * 100)
    print('Migration test')
    print('-' * 100)
    from utils.granular_storage.df.gran_df.sync import GranularSync
    from utils.granular_storage.df.gran_df import GranularMsgPackStorage as Storage
    from utils.os_tools import remove
    remove('granular_storage/gs_from/')
    remove('granular_storage/gs_to/')
    gs_from = Storage('granular_storage/gs_from')
    gs_to = Storage('granular_storage/gs_to')

    index = [cast_datetime('2018.05.01') + timedelta(hours=6) * i for i in range(3)]
    df = pd.DataFrame(np.random.randn(3, 1), index=index, columns=['0'])

    streamer = GranularStreamer(gs_to, 'sample_path', buffer_timeout=0, init_path=True, migration_expected=True)

    def print_table():
        try:
            print(gs_to['sample_path']['sample_table'])
        except:
            print('No table yet')

    streamer.append_stream(df.iloc[1:2], 'sample_table')
    print('Before migration')
    print_table()

    gran_from = gs_from.get_granular('sample_path', level=0)
    gran_from.append(df.iloc[:2], 'sample_table')

    # make migration happen
    sync = GranularSync(gs_from, gs_to)
    sync.sync('sample_path', 'sample_path', flag_migration=True)
    print('After migration')
    print_table()
    streamer.append_stream(df.iloc[2:3], 'sample_table')
    print('Stream streamed after migration')
    print_table()


if __name__ == '__main__':
    ex_stream()
    ex_migration()
