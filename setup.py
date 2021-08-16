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
    version='0.0.18',
    description='Vehicle Energy Consumption in Python: A tool to simulate load flexibility of electric vehicle fleets.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/dlr-ve/vencopy',
    author='Niklas Wulff',
    author_email='niklas.wulff@dlr.de',
    license='BSD 3-clause',
    packages=['vencopy', 'vencopy.classes', 'vencopy.scripts'],
    package_data={'': dataFilePaths},
    install_requires=['pandas >= 1.1.1, <= 1.2.5',
                      'ruamel',
                      'seaborn >= 0.9.0',
                      'sphinx >= 2.3.1',
                      'openpyxl >= 3.0.3',
                      'sphinx_rtd_theme >= 0.5.2',
                      # 'jupyterlab >= 3.1.0',
                      'Click >= 8.0.1',
                      'pyyaml >= 5.1.2'],
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
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering'],
)

