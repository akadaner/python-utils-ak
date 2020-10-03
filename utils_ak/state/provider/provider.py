class StateProvider:
    def get_state(self):
        pass

    def set_state(self, state):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
