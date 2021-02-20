import os
from uuid import uuid4

from utils_ak.architecture.integrity_guard.integrity_guard import IntegrityGuard
from utils_ak.os import remove_path, gen_tmp_fn

import logging

logger = logging.getLogger(__name__)


class FilesystemIntegrityGuard(IntegrityGuard):
    """ I'm an integrity guard. I check if some operation passed nicely or if it had crashed at some point. """

    def __init__(self, uuid=None):
        self.uuid = uuid or uuid4().hex

    @property
    def canary_path(self):
        return gen_tmp_fn(self.uuid, extension=".operation.canary")

    def is_integral(self):
        return not os.path.isfile(self.canary_path)

    def enter_unstable_state(self):  # Call me if you think that operation started
        logger.debug("Entering unstable state")
        open(self.canary_path, "w").close()  # Create a canary

    def leave_unstable_state(self):  # Call me if you think that operation ended
        logger.debug("Leaving unstable state")
        remove_path(self.canary_path)


if __name__ == "__main__":
    guard = FilesystemIntegrityGuard(uuid4().hex)
    guard.enter_unstable_state()  # Operation started, we are not integral!
    assert not guard.is_integral()
    guard.leave_unstable_state()  # We've finished nicely
    assert guard.is_integral()
