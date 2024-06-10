from utils_ak.block_tree.validation.validate_disjoint import validate_disjoint


def disjoint_validator(parent, block):
    axis = parent.props["axis"]
    for c in parent.children[:-1]:
        validate_disjoint(c, block, axis)

