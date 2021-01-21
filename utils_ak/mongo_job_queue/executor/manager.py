
class ExecutorManager:
    def start_executor(self, id, type, payload):
        pass

    def disable_executor(self, id):
        pass


class LocalExecutorManager(ExecutorManager):
    def __init__(self):
        self.executors = {}  # {id: thread}

    def start_executor(self, id, type, payload):
        pass

    def disable_executor(self, id):
        pass


class KubernetesExecutorManager(ExecutorManager):
    pass