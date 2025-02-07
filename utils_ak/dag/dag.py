"""LEGACY CODE (2025-02-07)"""

from collections import deque
from typing import Callable, Optional

from inline_snapshot import snapshot

from app.lessmore.utils.run_snapshot_tests.run_inline_snapshot_tests import run_inline_snapshot_tests


class DAGNode:
    def __init__(self):
        self.parents = []
        self.children = []

        self._iteration_state = {}
        self.reset_iteration_state()

    def reset_iteration_state(self, orient="down", with_children=False):
        self._iteration_state = {"is_processed": False}  # iteration state variable

        if with_children:
            for child in self.oriented_children(orient):
                child.reset_iteration_state(orient, with_children=True)

    def leaves(self, orient="down"):
        res = []
        for node in self.iterate_as_tree(orient):
            if not node.oriented_children(orient):
                res.append(node)
        return res

    def top(self, orient="down"):
        leaves = self.leaves(orient)
        assert len(leaves) == 1, "Top node not found"
        if len(leaves) == 1:
            return leaves[0]

    def root(self, orient="down"):
        cur_node = self

        while True:
            if not cur_node.oriented_children(orient):
                return cur_node
            else:
                cur_node = cur_node.oriented_children(orient)[0]

    def oriented_parents(self, orient="down"):
        return {"down": self.parents, "up": self.children}[orient]

    def oriented_children(self, orient="up"):
        return {"down": self.children, "up": self.parents}[orient]

    def iterate(self, orient="down"):
        """BFS"""
        assert not self.oriented_parents(orient), "Can iterate only from root node"
        self.reset_iteration_state(orient, with_children=True)

        cur_node = self

        while True:
            unprocessed_parents = [
                node for node in cur_node.oriented_parents(orient) if not node._iteration_state["is_processed"]
            ]
            if unprocessed_parents:
                cur_node = unprocessed_parents[0]
                continue

            yield cur_node
            cur_node._iteration_state["is_processed"] = True

            unprocessed_children = [
                node for node in cur_node.oriented_children(orient) if not node._iteration_state["is_processed"]
            ]

            if unprocessed_children:
                cur_node = unprocessed_children[0]
                continue
            else:
                break

    @staticmethod
    def _iter_node_as_tree_recursive(cur_node, orient="down"):
        if cur_node._iteration_state["is_processed"]:
            return
        else:
            yield cur_node
            cur_node._iteration_state["is_processed"] = True

        for child in cur_node.oriented_children(orient):
            for node in DAGNode._iter_node_as_tree_recursive(child, orient):
                yield node

    def iterate_as_tree(self, orient="down"):
        """DFS"""
        assert not self.oriented_parents(orient), "Can iterate only from root node"
        self.reset_iteration_state(orient, with_children=True)

        for node in self._iter_node_as_tree_recursive(self):
            yield node

    def schema(self, skip_rule: Optional[Callable] = None):
        """Prints the entire DAG from this node using BFS with proper chain handling."""
        result = ""
        visited = set()
        queue = deque([self])

        while queue:
            node = queue.popleft()

            if node in visited:
                continue

            visited.add(node)
            chain = [str(node)]

            while len(node.children) == 1 and len(node.children[0].parents) == 1:
                node = node.children[0]

                if skip_rule and skip_rule(node):
                    continue

                chain.append(str(node))
                visited.add(node)

            child_names = ", ".join(str(c) for c in node.children) if node.children else None

            if len(chain) + (1 if child_names else 0) >= 2:
                result += f"{' -> '.join(chain)}"
                if child_names:
                    result += f" -> [{child_names}]\n"
                else:
                    result += "\n"

            for child in node.children:
                if child not in visited:
                    queue.append(child)
        return result


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


def test():
    # - Init test dag node

    class NamedNode(DAGNode):
        def __init__(self, name):
            super().__init__()
            self.name = name

        def __str__(self):
            return self.name

        def __repr__(self):
            return self.name

    # - Test chain

    root = NamedNode("root")
    node1 = NamedNode("1")
    node2 = NamedNode("2")
    node3 = NamedNode("3")
    node4 = NamedNode("4")

    connect(root, node1)
    connect(node1, node2)
    connect(node2, node3)
    connect(node3, node4)

    assert root.schema() == snapshot("""\
root -> 1 -> 2 -> 3 -> 4
""")

    # - Test diamond: root_up -> node1, node2 -> root_down

    # -- Init diamond

    root_up = NamedNode("root_up")
    node1 = NamedNode("1")
    node2 = NamedNode("2")
    root_down = NamedNode("root_down")

    connect(root_up, node1)
    connect(root_up, node2)
    connect(node1, root_down)
    connect(node2, root_down)

    assert root_up.schema() == snapshot("""\
root_up -> [1, 2]
1 -> [root_down]
2 -> [root_down]
""")

    # -- Test iteration

    assert [str(node) for node in root_up.iterate(orient="down")] == ["root_up", "1", "2", "root_down"]
    assert [str(node) for node in root_down.iterate(orient="up")] == ["root_down", "1", "2", "root_up"]

    for node in [node1, node2]:
        try:
            node.iterate(orient="down")
        except AssertionError as e:
            assert str(e) == "Can iterate only from root node"

        try:
            node.iterate(orient="up")
        except AssertionError as e:
            assert str(e) == "Can iterate only from root node"

    # -- Test root

    for node in [root_up, node1, node2, root_down]:
        assert node.root(orient="up") == root_up
        assert node.root(orient="down") == root_down

    # - Test tree

    # -- Init tree

    root_up = NamedNode("root_up")
    node1 = NamedNode("1")
    node2 = NamedNode("2")

    connect(root_up, node1)
    connect(root_up, node2)

    assert root_up.schema() == snapshot("""\
root_up -> [1, 2]
""")

    # -- Test leaves

    assert list(root_up.leaves()) == [node1, node2]

    # -- Test top

    try:
        print(root_up.top())
    except AssertionError as e:
        assert str(e) == "Top node not found"

    root_down = NamedNode("root_down")
    for leaf in root_up.leaves():
        connect(leaf, root_down)

    assert root_up.top() == root_down
    assert list(root_up.leaves()) == [root_down]
    assert list(root_up.iterate_as_tree()) == [root_up, node1, root_down, node2]


if __name__ == "__main__":
    run_inline_snapshot_tests(mode="update_all")
