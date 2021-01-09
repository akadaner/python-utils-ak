from utils_ak.serialization import cast_js

from utils_ak.fluid_flow.event_manager import EVENT_MANAGER

class FluidFlow:
    def __init__(self, root):
        self.root = root

    def __str__(self):
        values = ['Primitive Flow']
        for node in self.root.iterate('down'):
            values.append(' ' * 4 + str(node) + ': ' + cast_js(node.stats()))
        return '\n'.join(values)

    def update(self, topic, ts, event):
        print('Processing time', ts)

        print('Updating value')
        for node in self.root.iterate('down'):
            getattr(node, 'update_value', lambda ts: None)(ts)
        print(self)

        print('Updating pressure')
        for node in self.root.iterate('down'):
            getattr(node, 'update_pressure', lambda ts: None)(ts)
        print(self)

        print('Updating speed')
        for node in self.root.iterate('down'):
            getattr(node, 'update_speed', lambda ts: None)(ts)
        print(self)

        print('Updating triggers')
        for node in self.root.iterate('down'):
            getattr(node, 'update_triggers', lambda ts: None)(ts)
        print(self)

        print('Updating last ts')
        for node in self.root.iterate('down'):
            getattr(node, 'update_last_ts', lambda ts: None)(ts)
        print(self)
        print()


def run_flow(flow):
    EVENT_MANAGER.subscribe('', flow.update)
    EVENT_MANAGER.add_event('update', 0, {})
    EVENT_MANAGER.run()

