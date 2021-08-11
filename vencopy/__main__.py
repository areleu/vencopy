__version__ = "0.0.9"
__author__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Benjamin Fuchs'
__credits__ = 'German Aerospace Center (DLR)'
__license__ = 'BSD-3-Clause'

import click
import os
import shutil
import pathlib
import vencopy


@click.command()
@click.option("--name", default='vencopy_user', prompt="Please type the user folder name:",
              help="The folder name of the vencopy user folder created at command line working directory")
@click.option("--tutorials", default='true', help='Specify if tutorials should be copied to the user folder on set up. '
                                                  'Defaults to true')
# @click.option("--dir", default='', help='Specify separate aboslute path where the user folder should be set up')
def create(name: str, tutorials: bool):
    """VencoPy folder set up after installation"""
    cwd = pathlib.Path(os.getcwd())
    target = cwd / name
    source = pathlib.Path(vencopy.__file__).parent.resolve()
    if not os.path.exist(target):
        os.mkdir(target)
        setupFolders(src=target, trg=tutorials)
    elif os.path.exist(target) and not os.path.exist(target / 'run.py'):
        setupFolders(src=source, trg=target)
    else:
        click.echo('VencoPy user folder is already set up')


def setupFolders(src: pathlib.Path, trg: pathlib.Path, tutorials: bool):
    """
    Setup function to create a vencopy user folder and to copy run, config and tutorial files from the package source

    :param src: Absolute path to the vencopy package source folder
    :param trg: Absolute path to the vencopy user folder
    :param tutorials: Boolean, if true (default) tutorials are being copied from package source to user folder
    :return: None
    """
    os.mkdir(trg / 'inputData')
    os.mkdir(trg / 'output')
    shutil.copy(src=src / 'run.py', dst=trg)
    shutil.copy(src=src / 'config', dst=trg)
    if tutorials:
        shutil.copy(src=src / 'tutorials', dst=trg)

if __name__ == '__main__':
    create()



