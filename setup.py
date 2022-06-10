__version__ = '0.1.X'
__author__ = 'Niklas Wulff'
__contributors__ = 'Benjamin Fuchs'
__credits__ = 'German Aerospace Center (DLR)'
__license__ = 'BSD-3-Clause'

import os
import pathlib
from setuptools import setup, find_packages


def walkDataFiles(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths


dataFilePaths = walkDataFiles('./vencopy/config')
dataFilePaths.extend(walkDataFiles('./vencopy/tutorials'))
long_description = (pathlib.Path(__file__).parent.resolve() / 'README.md').read_text(encoding='utf-8')
setup(
    name='vencopy',
    version='0.1.5',
    description='Vehicle Energy Consumption in Python: A tool to simulate load flexibility of electric vehicle fleets.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/dlr-ve/vencopy',
    author='Niklas Wulff',
    author_email='niklas.wulff@dlr.de',
    license='BSD 3-clause',
    packages=['vencopy', 'vencopy.core', 'vencopy.utils'],
    package_data={'': dataFilePaths},
    install_requires=['pandas >= 1.1.1, <= 1.2.5',
                      'ruamel.yaml',
                      'seaborn',
                      'openpyxl',
                      'jupyterlab',
                      'Click',
                      'pyyaml'],
    entry_points={
        'console_scripts': [
            'vencopy = vencopy.__main__:create',
        ],
    },

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: Microsoft :: Windows :: Windows 10',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering'],
)

