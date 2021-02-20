import torch.nn as nn
import torch.utils.data
import numpy as np

from .images import *


class DynamicNet(nn.Module):
    """
    Pytorch net that constructs during first forward pass

    Sample Usage:

    class LeNet(DynamicNet):
        def _forward(self, x):
            x = self.layer('Conv2d', 1, 20, 5, 1)(x)
            x = F.relu(x)
            x = F.max_pool2d(x, 2, 2)

            x = self.layer('Conv2d', 20, 50, 5, 1)(x)
            x = F.relu(x)
            x = F.max_pool2d(x, 2, 2)

            x = x.view(-1, 4*4*50)
            x = self.layer('Linear', 4 * 4 * 50, 500)(x)
            x = F.relu(x)
            x = self.layer('Linear', 500, 10)(x)
            return x

    model = LeNet()
    # init net with one forward pass
    model.forward(Variable(X_test))

    # now model.parameters() is not empty
    """

    def __init__(self):
        super().__init__()
        self.init = True
        self.layer_counter = {}

    def forward(self, x):
        res = self._forward(x)
        self.init = False
        self.layer_counter = {}
        return res

    def layer(self, name, *args, **kwargs):
        if self.init:
            layer_count = 0
            while True:
                if not hasattr(self, f"{name}_{layer_count}"):
                    break
                layer_count += 1

            if hasattr(nn, name):
                res = getattr(nn, name)(*args, **kwargs)
            elif hasattr(self, name):
                res = getattr(self, name)(*args, **kwargs)
            else:
                raise Exception(f"Layer not found: {name}]")
            setattr(self, f"{name}_{layer_count}", res)
            return res
        else:
            layer_count = self.layer_counter.get(name, -1) + 1
            self.layer_counter[name] = layer_count
            return getattr(self, f"{name}_{layer_count}")

    def print_init(self, *args):
        if self.init:
            print(*args)


class NPYDataset(torch.utils.datasets.Dataset):
    def __init__(
        self,
        images_path,
        labels_path=None,
        transform=None,
        train=True,
        train_size=0.9,
        shuffle=False,
        seed=12,
    ):
        self.images = np.load(images_path)
        if labels_path:
            self.labels = np.load(labels_path)
        else:
            self.labels = None
        self.transform = transform

        self.total_size = self.images.shape[0]
        if isinstance(train_size, int):
            self.train_size = train_size
        else:
            self.train_size = int(self.total_size * train_size)
        self.train = train

        if not shuffle:
            train_idxs = np.arange(0, self.train_size)
            test_idxs = np.arange(self.train_size, self.total_size)
        else:
            np.random.seed(seed)
            permutation = np.random.permutation(self.total_size)
            train_idxs = permutation[: self.train_size]
            test_idxs = permutation[self.train_size :]

        if self.train:
            self.images = self.images[train_idxs, :, :]
            if self.labels is not None:
                self.labels = self.labels[train_idxs]
        else:
            self.images = self.images[test_idxs, :, :]
            if self.labels is not None:
                self.labels = self.labels[test_idxs]

    def __getitem__(self, index):
        im = self.images[index, :, :]

        if self.labels is not None:
            label = self.labels[index]
        else:
            label = 0

        if self.transform is not None:
            im = cast_pytorch_image(im)
            im = cast_image(im)
            im = self.transform(im)
        return im, label

    def __len__(self):
        return self.images.shape[0]


def collect_images_to_numpy(data_loader):
    data = None
    labels = None
    for cur_data, cur_labels in data_loader:
        data_numpy = (cur_data * 255).numpy().astype("uint8")
        labels_numpy = cur_labels.numpy()
        if data is None:
            data = data_numpy
            labels = labels_numpy
        else:
            data = np.concatenate([data, data_numpy])
            labels = np.concatenate([labels, labels_numpy])

    return data, labels


import logging
