

class Updater:
    def __init__(self, state_provider, job_runner, start_id):
        self.state_provider = state_provider
        self.job_runner = job_runner
        self.start_id = start_id

    def generate_jobs(self, last_id):
        pass

    def process(self, job):
        pass

    def combine(self, outputs):
        pass

    def apply(self, update):
        pass

    def update(self):
        state = self.state_provider.get_state()
        last_id = state.get('last_id', self.start_id)
        jobs = self.generate_jobs(last_id)
        outputs = self.job_runner.run_jobs(jobs)
        new_state, update = self.combine(outputs)
        self.apply(update)
        self.state_provider.set_state(new_state)


