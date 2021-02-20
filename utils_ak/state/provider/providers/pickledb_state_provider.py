import os
import pickledb

from utils_ak.state.provider.provider import StateProvider
from utils_ak.os import *


class PickleDBStateProvider(StateProvider):
    """ I keep a number (which is a called a state). I allow for the state to not change in case of crash. """

    def __init__(self, fn, key="state"):
        super().__init__()
        self.key = key

        self.fn = fn
        makedirs(fn)
        self.canary_fn = fn + ".canary"

        self.db = pickledb.load(fn, auto_dump=False)

        self.state = None

        self._recover()

    def _recover(self):
        if os.path.exists(self.canary_fn):
            self.canary_db = pickledb.load(self.canary_fn, auto_dump=False)
            self.set_state(self.canary_db.get(self.key))
            remove_path(self.canary_fn)

    def get_state(self):
        if self.state is None:
            self.state = self.db.get(self.key) or {}
            return self.state
        return self.state

    def set_state(self, state):
        self.state = state
        self.db.set(self.key, state)
        self.db.dump()

    def __enter__(self):
        self.canary_db = pickledb.load(self.canary_fn, auto_dump=False)
        self.canary_db.set(self.key, self.get_state())
        self.canary_db.dump()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        remove_path(self.canary_fn)


def test_pickle_db_state_provider():
    state_provider = PickleDBStateProvider("state.pickledb")
    state_provider.set_state(16)
    assert state_provider.get_state() == 16

    PickleDBStateProvider("state.pickledb").set_state(17)
    assert PickleDBStateProvider("state.pickledb").get_state() == 17

    with PickleDBStateProvider("state.pickledb") as sp:
        assert sp.get_state() == 17
        sp.set_state(18)
        assert sp.get_state() == 18
    print(state_provider.get_state())

    # clean up
    from utils_ak import remove_path

    remove_path("state.pickledb")


if __name__ == "__main__":
    test_pickle_db_state_provider()
