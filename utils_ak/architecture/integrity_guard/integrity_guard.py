class IntegrityGuard(object):
    """
    I'm an abstraction. Extend me.

    I check if some operation has gone good or has crashed anywhere in the past.
    Usually, I raise some flags before entering the unstable area, and put it down upon leaving it.
    If I find a risen flag -- we've crashed at some point and need to take some actions.
    Those actions are out of our scope -- we just check.
    """

    def is_integral(self):  # Are we integral at the moment? Is there a risen flag?
        raise NotImplementedError()

    def enter_unstable_state(
        self,
    ):  # Boom, we are unstable now. integral() must be false after calling this
        raise NotImplementedError()

    def leave_unstable_state(self):  # Hurray, we're done, kill the flag!
        raise NotImplementedError()
