from setuptools import find_packages

# cython solution from here: http://stackoverflow.com/questions/4505747/how-should-i-structure-a-python-package-that-contains-cython-code
from distutils.core import setup
from distutils.extension import Extension

from distutils.command.sdist import sdist as _sdist

import numpy as np

try:
    from Cython.Distutils import build_ext
except ImportError:
    use_cython = False
else:
    use_cython = True

cmdclass = {}
ext_modules = []


setup(name='utils',
      version='1.0.0',
      description='Quantribution Library',
      packages=find_packages(),
      cmdclass=cmdclass,
      ext_modules=ext_modules,
      install_requires=['joblib', 'h5py', 'anyconfig', 'retrypy', 'pymongo', 'slacker', 'requests-futures', 'empyrical',
                        'filelock'],
      include_dirs=[np.get_include()],
      zip_safe=False)
