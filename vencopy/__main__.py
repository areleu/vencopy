__version__ = '0.1.X'
__author__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Benjamin Fuchs'
__email__ = 'niklas.wulff@dlr.de'
__credits__ = 'German Aerospace Center (DLR)'
__license__ = 'BSD-3-Clause'

import os
import yaml
import shutil
import pathlib
import click
import vencopy


# @click.option("--dir", default='', help='Specify separate aboslute path where the user folder should be set up')
@click.command()
@click.option("--name", default='vencopy_user', prompt="Please type the user folder name:",
              help="The folder name of the vencopy user folder created at command line working directory")
@click.option("--tutorials", default='true', help='Specify if tutorials should be copied to the user folder on set up. '
                                                  'Defaults to true')
def create(name: str, tutorials: bool):
    """VencoPy folder set up after installation"""
    cwd = pathlib.Path(os.getcwd())
    target = cwd / name
    source = pathlib.Path(vencopy.__file__).parent.resolve()
    if not os.path.exists(target):
        os.mkdir(target)
        setupFolders(src=source, trg=target, tutorials=tutorials)
        click.echo(f'VencoPy user folder created under {target}')
    elif os.path.exists(target) and not os.path.exists(target / 'run.py'):
        setupFolders(src=source, trg=target, tutorials=tutorials)
        click.echo(f'VencoPy user folder filled under {target}')
    else:
        click.echo('File run.py already exists in specified folder, for a new setup please specify a non-existent '
                   'folder name')


def setupFolders(src: pathlib.Path, trg: pathlib.Path, tutorials: bool):
    """
    Setup function to create a vencopy user folder and to copy run, config and tutorial files from the package source

    :param src: Absolute path to the vencopy package source folder
    :param trg: Absolute path to the vencopy user folder
    :param tutorials: Boolean, if true (default) tutorials are being copied from package source to user folder
    :return: None
    """
    os.mkdir(trg / 'output')
    os.mkdir(trg / 'output' / 'dataParser')
    os.mkdir(trg / 'output' / 'diaryBuilder')
    os.mkdir(trg / 'output' / 'gridModeler')
    os.mkdir(trg / 'output' / 'flexEstimator')
    os.mkdir(trg / 'output' / 'evaluator')
    shutil.copy(src=src / 'run.py', dst=trg)
    shutil.copytree(src=src / 'config', dst=trg / 'config')
    if tutorials:
        shutil.copytree(src=src / 'tutorials', dst=trg / 'tutorials')
    updateLocalPathCfg(newVPRoot=trg)


def updateLocalPathCfg(newVPRoot: pathlib.Path):
    with open(newVPRoot / 'config' / 'localPathConfig.yaml') as f:
        pathCfg = yaml.load(f, Loader=yaml.SafeLoader)

    pathCfg['pathAbsolute']['vencoPyRoot'] = newVPRoot.__str__()

    with open(newVPRoot / 'config' / 'localPathConfig.yaml', 'w') as f:
        yaml.dump(pathCfg, f)


if __name__ == '__main__':
    create()
