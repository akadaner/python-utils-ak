class DAGNode:
    def __init__(self):
        self.parents = []
        self.children = []

        self._iteration_state = {}
        self.reset_iteration_state()

    def reset_iteration_state(self, orient='down', with_children=False):
        self._iteration_state = {'is_processed': False}  # iteration state variable

        if with_children:
            for child in self.oriented_children(orient):
                child.reset_iteration_state(orient, with_children=True)

    def leaves(self, orient='down'):
        res = []
        for node in self.iterate_as_tree(orient):
            if not node.oriented_children(orient):
                res.append(node)
        return res

    def top(self, orient='down'):
        leaves = self.leaves(orient)
        assert len(leaves) == 1, 'Top node not found'
        if len(leaves) == 1:
            return leaves[0]

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
            unprocessed_parents = [node for node in cur_node.oriented_parents(orient) if not node._iteration_state['is_processed']]
            if unprocessed_parents:
                cur_node = unprocessed_parents[0]
                continue

            yield cur_node
            cur_node._iteration_state['is_processed'] = True

            unprocessed_children = [node for node in cur_node.oriented_children(orient) if not node._iteration_state['is_processed']]

            if unprocessed_children:
                cur_node = unprocessed_children[0]
                continue
            else:
                break

    @staticmethod
    def _iter_node_as_tree_recursive(cur_node, orient='down'):
        if cur_node._iteration_state['is_processed']:
            return
        else:
            yield cur_node
            cur_node._iteration_state['is_processed'] = True

        for child in cur_node.oriented_children(orient):
            for node in DAGNode._iter_node_as_tree_recursive(child, orient):
                yield node

    def iterate_as_tree(self, orient='down'):
        assert not self.oriented_parents(orient), 'Can iterate only from root node'
        self.reset_iteration_state(orient, with_children=True)

        for node in self._iter_node_as_tree_recursive(self):
            yield node


def connect(parent, child, safe=True):
    if safe and (not parent or not child):
        return
    parent.children.append(child)
    child.parents.append(parent)


def disconnect(parent, child, safe=True):
    if safe and (not parent or not child):
        return
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

    print('Testing Leaves, top, iterate as tree')
    root_up = NamedNode('root_up')
    node1 = NamedNode('1')
    node2 = NamedNode('2')

    connect(root_up, node1)
    connect(root_up, node2)
    print(list(root_up.leaves()))
    try:
        print(root_up.top())
    except AssertionError as e:
        print('AssertionError', e)
    top = NamedNode('Top')
    for leaf in root_up.leaves():
        connect(leaf, top)
    print(root_up.top())
    print(list(root_up.leaves()))

    for node in root_up.iterate():
        print(node)

    for node in root_up.iterate_as_tree():
        print(node)


if __name__ == '__main__':
    test_dag_iteration()