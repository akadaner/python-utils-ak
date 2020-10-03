import numpy as np

from matplotlib import pyplot as plt

import torch
import torchvision
import torchvision.transforms as transforms

import PIL
from PIL import Image
import scipy.misc
import cv2
import os


# all images are in RGB by default
# last dimension is color by default (1 or 3). In pytorch - this is the first dimension


def cast_numpy(im):
    if isinstance(im, np.ndarray):
        if len(im.shape) == 2:
            return im
        elif len(im.shape) == 3:
            if im.shape[-1] not in [1, 3] and im.shape[0] in [1, 3]:
                im = np.transpose(im, (1, 2, 0))
        else:
            raise Exception('Not an image')
        return im
    elif isinstance(im, PIL.Image.Image):
        return np.asarray(im)
    elif isinstance(im, torch.Tensor):
        return cast_numpy(im.numpy())
    elif isinstance(im, str) and os.path.exists(im):
        im = cv2.imread(im)
        im = cv2.cvtColor(im, cv2.COLOR_BGR2RGB)
        return im
    else:
        raise Exception('Unknown image type')


def cast_pytorch_image(im):
    if not isinstance(im, np.ndarray):
        im = cast_numpy(im)

    if len(im.shape) != 3:
        raise NotImplementedError('Not implemented')

    if im.shape[0] not in [1, 3]:
        if im.shape[-1] in [1, 3]:
            im = np.transpose(im, (2, 0, 1))
        else:
            raise Exception('Not an image')
    return im


def cast_image(im):
    if isinstance(im, np.ndarray) and len(im.shape) == 3 and im.shape[0] == 1:
        im = im[0]
    else:
        im = cast_numpy(im)
    return Image.fromarray(im)


def cast_grayscale(im):
    if len(im.shape) > 2 and im.shape[2] == 3:
        im = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    else:
        #         im = cv2.imdecode(im, cv2.CV_LOAD_IMAGE_GRAYSCALE)
        return im
    return im


def show(im):
    im = cast_numpy(im)
    if len(im.shape) == 2:
        plt.imshow(im, cmap='gray')
    else:
        plt.imshow(im)


def save(im, fn):
    scipy.misc.imsave(fn, im)


def resize(im, new_shape, interpolation=cv2.INTER_CUBIC):
    im = cv2.resize(im, new_shape, interpolation=interpolation)
    return im


def flip(im):
    return cv2.flip(im, 1)
