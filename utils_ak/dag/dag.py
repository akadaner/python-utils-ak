class DAGNode:
    def __init__(self):
        self.parents = []
        self.children = []
        self.reset_iteration_state()

    def reset_iteration_state(self, orient='down', with_children=False):
        self.iteration_state = {'is_processed': False}  # iteration state variable

        if with_children:
            for child in self.oriented_children(orient):
                child.reset_iteration_state(orient, with_children=True)

    def root(self, orient='down'):
        cur_node = self

        while True:
            if not cur_node.oriented_children(orient):
                return cur_node
            else:
                cur_node = cur_node.oriented_children(orient)[0]

    def oriented_parents(self, orient='down'):
        return {'down': self.parents, 'up': self.children}[orient]

    def oriented_children(self, orient='upwards'):
        return {'down': self.children, 'up': self.parents}[orient]

    def iterate(self, orient='down'):
        assert not self.oriented_parents(orient), 'Can iterate only from root node'
        self.reset_iteration_state(orient, with_children=True)

        cur_node = self

        while True:
            unprocessed_parents = [node for node in cur_node.oriented_parents(orient) if not node.iteration_state['is_processed']]
            if unprocessed_parents:
                cur_node = unprocessed_parents[0]
                continue

            yield cur_node
            cur_node.iteration_state['is_processed'] = True

            unprocessed_children = [node for node in cur_node.oriented_children(orient) if not node.iteration_state['is_processed']]

            if unprocessed_children:
                cur_node = unprocessed_children[0]
                continue
            else:
                break


def connect(parent, child):
    parent.children.append(child)
    child.parents.append(parent)


def disconnect(parent, child):
    parent.children.remove(child)
    child.parents.remove(parent)


def test_dag_iteration():
    class NamedNode(DAGNode):
        def __init__(self, name):
            super().__init__()
            self.name = name

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.name

    root_up = NamedNode('root_up')
    node1 = NamedNode('1')
    node2 = NamedNode('2')
    root_down = NamedNode('root_down')

    connect(root_up, node1)
    connect(root_up, node2)
    connect(node1, root_down)
    connect(node2, root_down)

    print('Testing iteration')
    for orient in ['down', 'up']:
        print('Processing orientation', orient)
        for node in [root_up, node1, node2, root_down]:
            print('Processing node', node)
            if orient == 'down' and node == root_up:
                for iter_node in node.iterate(orient):
                    print(iter_node)
            elif orient == 'up' and node == root_down:
                for iter_node in node.iterate(orient):
                    print(iter_node)
            else:
                try:
                    list(node.iterate(orient))
                except AssertionError as e:
                    print('AssertionError', e)

    print('Testing root')
    for orient in ['down', 'up']:
        print('Processing orientation', orient)
        for node in [root_up, node1, node2, root_down]:
            print(node.root(orient))

if __name__ == '__main__':
    test_dag_iteration()