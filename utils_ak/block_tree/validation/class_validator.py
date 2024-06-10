from utils_ak.str import trim


class ClassValidator:
    def __init__(self, window=2, window_by_classes=None):
        self.validators = {}
        self.window = window
        self.window_by_classes = window_by_classes or {}

        # add attribute validations
        validate_attrs = [attr for attr in dir(self) if attr.startswith("validate__")]

        for attr in validate_attrs:
            class1, class2 = trim(attr, "validate__").split("__")
            self.add(class1, class2, getattr(self, attr))

    def add(self, class1, class2, validator):
        self.validators[(class1, class2)] = validator

    def validate(self, b1, b2):
        key = (b1.props["cls"], b2.props["cls"])
        if key in self.validators:
            self.validators[key](b1, b2)

    def __call__(self, parent, block):
        parent_blocks = parent.children[-self.window - 1 : -1]

        if not parent_blocks:
            return

        b2 = block

        if not self.window_by_classes.get(block.props["cls"]):
            for b1 in parent_blocks:
                self.validate(b1, b2)
        else:
            parent_classes = set([b.props["cls"] for b in parent_blocks])
            for parent_cls in parent_classes:
                if parent_cls not in self.window_by_classes[block.props["cls"]]:

                    # don't check at all
                    continue
                cls_parent_blocks = [b for b in parent_blocks if b.props["cls"] == parent_cls]
                cls_parent_blocks = cls_parent_blocks[-self.window_by_classes[block.props["cls"]][parent_cls] :]

                for b1 in cls_parent_blocks:
                    self.validate(b1, b2)


def test():
    class_validator = ClassValidator(window=1)
    class_validator.add("a", "a", validator=validate_disjoint)

    root = ParallelepipedBlock("root", axis=0)
    a1 = ParallelepipedBlock("a", n_dims=2, x=[0, 0], size=[5, 1])
    a2 = ParallelepipedBlock("a", n_dims=2, x=[2, 0], size=[5, 1])
    b = ParallelepipedBlock("b", n_dims=2, x=[0, 1], size=[5, 1])
    root.add_child(a1)

    try:
        class_validator(root, a2)
    except AssertionError as e:
        print(e)

    class_validator(root, b)

    root.add_child(b)

    # window is 1 - validation should pass now
    class_validator(root, a2)

    root.add_child(a2)