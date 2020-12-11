import os
import pickledb

from utils_ak.state.provider.provider import StateProvider
from utils_ak.os import make_directories


class PickleDBStateProvider(StateProvider):
    """ I keep a number (which is a called a state). I allow for the state to not change in case of crash. """

    def __init__(self, fn, state_key='state', default_state=None):
        super().__init__()

        self.state_key = state_key
        self.fn = fn
        make_directories(self.fn)
        # crash canary
        self.canary_name = fn + ".canary"

        self.db = pickledb.load(fn, False)

        self.default_state = default_state or {}
        self.state = None
        self._recover()

    def _recover(self):
        # Check if canary is there
        if os.path.isfile(self.canary_name):
            # We have crashed while updating state, recovering the state
            self.canary_db = pickledb.load(self.canary_name, False)
            self.set_state(self.canary_db.get(self.state_key) or self.default_state)
            os.remove(self.canary_name)

    def get_state(self):
        if self.state is None:
            self.state = self.db.get(self.state_key) or self.default_state
        return self.state

    def set_state(self, state):
        self.state = state
        self.db.set(self.state_key, state)
        self.db.dump()  # Dumping instantly

    def __enter__(self):
        self.canary_db = pickledb.load(self.canary_name, False)
        self.canary_db.set(self.state_key, self.get_state())
        self.canary_db.dump()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.canary_name)


if __name__ == "__main__":
    storage = PickleDBStateProvider('state.pickledb')
    storage.set_state(16)
    assert storage.get_state() == 16

    PickleDBStateProvider('state.pickledb').set_state(17)
    assert PickleDBStateProvider('state.pickledb').get_state() == 17

    with PickleDBStateProvider('state.pickledb') as sp:
        assert sp.get_state() == 17
        sp.set_state(18)
        assert sp.get_state() == 18
    print(storage.get_state())
