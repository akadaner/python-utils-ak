import logging

from utils_ak.dag import *
from utils_ak.serialization import cast_js
from utils_ak.simple_event_manager import SimpleEventManager

from utils_ak.fluid_flow.actor import Actor
from utils_ak.fluid_flow.actors import pipe_together, Stub


class FluidFlow:
    def __init__(self, root, verbose=False):
        self.root = root

        # create top if needed
        if len(self.root.leaves()) > 1:
            top = Stub('Top')
            for i, leaf in enumerate(self.root.leaves()):
                pipe_together(leaf, top, f'Top parent {i}')

        self.verbose = verbose
        self.logger = logging.getLogger()

    def __str__(self):
        values = ['Flow:']
        for node in self.root.iterate('down'):
            values.append(' ' * 4 + str(node) + ': ' + cast_js(node.stats()))
        return '\n'.join(values)

    def __repr__(self):
        return str(self)

    def log(self, *args):
        if self.verbose:
            # self.logger.debug(args)
            print(*args)

    def update(self, topic, ts, event):
        self.log('Processing time', ts)

        for method in ['update_value', 'update_pressure', 'update_speed', 'update_triggers', 'update_last_ts']:
            # self.log(f'Procedure {method}')
            for node in self.root.iterate('down'):
                getattr(node, method, lambda ts: None)(ts)
            # self.log(self)
        self.log(self)

        self.log()



def run_flow(flow):
    event_manager = SimpleEventManager()
    for node in flow.root.iterate('down'):
        node.set_event_manager(event_manager)
        node.subscribe()

    event_manager.subscribe('', flow.update)
    event_manager.add_event('update', 0, {})
    event_manager.run()
