class DefaultJobRunner:
    def run_jobs(self, worker, jobs):
        return [worker(job) for job in jobs]


class Updater:
    def __init__(self, state_provider, start_id, job_runner=None):
        self.state_provider = state_provider
        self.job_runner = job_runner or DefaultJobRunner()
        self.start_id = start_id

    def init(self):
        pass

    def generate_jobs(self, last_id):
        raise NotImplementedError

    def combine(self, outputs):
        raise NotImplementedError

    def apply(self, update):
        raise NotImplementedError

    def update(self, worker):
        self.init()
        state = self.state_provider.get_state()
        last_id = state.get('last_id', self.start_id)
        jobs = self.generate_jobs(last_id)
        outputs = self.job_runner.run_jobs(worker, jobs)
        new_state, update = self.combine(outputs)
        self.apply(update)
        self.state_provider.set_state(new_state)
