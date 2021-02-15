from datetime import datetime

from utils_ak.updater.updater import Updater


class SampleUpdater(Updater):
    def generate_jobs(self, last_id):
        return range(int(last_id) + 1, int(datetime.now().timestamp()))

    def process(self, job):
        print('Processing job', job)
        return job

    def combine(self, outputs):
        print('Combining outputs', outputs)
        return {'last_id': max(outputs)}, ','.join([str(output) for output in outputs])

    def apply(self, update):
        print('Applying update', update)


def test_sample_updater():
    from utils_ak.state.provider import PickleDBStateProvider
    class JobRunner:
        def run_jobs(self, jobs):
            return jobs
    su = SampleUpdater(PickleDBStateProvider('state.pickle'), JobRunner(), start_id=int(datetime.now().timestamp()) - 10)
    su.update()


if __name__ == '__main__':
    test_sample_updater()