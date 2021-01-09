from utils_ak.serialization import cast_js

from utils_ak.fluid_flow.event_manager import SimpleEventManager
import logging


class FluidFlow:
    def __init__(self, root, verbose=False):
        self.root = root
        self.verbose = verbose
        self.logger = logging.getLogger()

    def __str__(self):
        values = ['Primitive Flow']
        for node in self.root.iterate('down'):
            values.append(' ' * 4 + str(node) + ': ' + cast_js(node.stats()))
        return '\n'.join(values)

    def __repr__(self):
        return str(self)

    def log(self, *args):
        if self.verbose:
            # self.logger.info(args)
            print(*args)

    def update(self, topic, ts, event):
        self.log('Processing time', ts)

        self.log('Updating value')
        for node in self.root.iterate('down'):
            getattr(node, 'update_value', lambda ts: None)(ts)
        self.log(self)

        self.log('Updating pressure')
        for node in self.root.iterate('down'):
            getattr(node, 'update_pressure', lambda ts: None)(ts)
        self.log(self)

        self.log('Updating speed')
        for node in self.root.iterate('down'):
            getattr(node, 'update_speed', lambda ts: None)(ts)
        self.log(self)

        self.log('Updating triggers')
        for node in self.root.iterate('down'):
            getattr(node, 'update_triggers', lambda ts: None)(ts)
        self.log(self)

        self.log('Updating last ts')
        for node in self.root.iterate('down'):
            getattr(node, 'update_last_ts', lambda ts: None)(ts)
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

