import logging
logger = logging.getLogger(__name__)


class FiniteStateMachine:
    _states = ['blank']
    _require_state_callbacks = False

    def __init__(self, state='blank'):
        self.state = state

    def set_state(self, state):
        if state not in self._states:
            raise Exception('Unknown status')

        if state != self.state:
            self.on_state_change(self.state, state)

        self.state = state

    def on_state_change(self, old_state, new_state):
        logger.debug(f'State change: {old_state} -> {new_state}')
        state_callback = getattr(self, f'on_{new_state}', None)
        if not state_callback:
            if self._require_state_callbacks:
                raise Exception(f'Callback for state {new_state} is not defined')
        else:
            state_callback()

    def get_state(self):
        raise NotImplementedError

