__version__ = "1.0.X"
__author__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli, Benjamin Fuchs"
__email__ = "niklas.wulff@dlr.de"
__credits__ = "German Aerospace Center (DLR)"
__license__ = "BSD-3-Clause"

import os
import yaml
import shutil
import pathlib
import click
import vencopy


@click.command()
@click.option(
    "--name",
    default="vencopy_user",
    prompt="Please type the user folder name:",
    help="The folder name of the venco.py user folder created at command line working directory",
)
# Tutorials will be updated on next release
@click.option(
    "--tutorials",
    default="false",
    help="Specify if tutorials should be copied to the user folder on set up. " "Defaults to true",
)
def create(name: str, tutorials: bool):
    """venco.py folder set up after installation"""
    cwd = pathlib.Path(os.getcwd())
    target = cwd / name
    source = pathlib.Path(vencopy.__file__).parent.resolve()
    if not os.path.exists(target):
        os.mkdir(target)
        setupFolders(src=source, trg=target, tutorials=tutorials)
        click.echo(f"venco.py user folder created under {target}")
    elif os.path.exists(target) and not os.path.exists(target / "run.py"):
        setupFolders(src=source, trg=target, tutorials=tutorials)
        click.echo(f"venco.py user folder filled under {target}")
    else:
        click.echo(
            "File run.py already exists in specified folder, for a new setup please specify a non-existent "
            "folder name"
        )


def setupFolders(src: pathlib.Path, trg: pathlib.Path, tutorials: bool):
    """
    Setup function to create a vencopy user folder and to copy run, config and tutorial files from the package source.

    :param src: Absolute path to the vencopy package source folder
    :param trg: Absolute path to the vencopy user folder
    :param tutorials: Boolean, if true (default) tutorials are being copied from package source to user folder
    :return: None
    """
    os.mkdir(trg / "config")
    os.mkdir(trg / "core")
    os.mkdir(trg / "output")
    os.mkdir(trg / "output" / "dataParser")
    os.mkdir(trg / "output" / "diaryBuilder")
    os.mkdir(trg / "output" / "gridModeler")
    os.mkdir(trg / "output" / "flexEstimator")
    os.mkdir(trg / "output" / "postProcessor")
    os.mkdir(trg / "utils")
    shutil.copy(src=src / "run.py", dst=trg)
    shutil.copytree(src=src / "config", dst=trg / "config")
    if tutorials:
        raise (NotImplementedError("Tutorials for the new venco.py iteration are not yet implemented."))
        # shutil.copytree(src=src / "tutorials", dst=trg / "tutorials")
    updateLocalPathCfg(newVPRoot=trg)


def updateLocalPathCfg(newVPRoot: pathlib.Path):
    with open(newVPRoot / "config" / "user.yaml") as f:
        user_cfg = yaml.load(f, Loader=yaml.SafeLoader)

    user_cfg["absolute_path"]["vencopy_root"] = newVPRoot.__str__()

    with open(newVPRoot / "config" / "user.yaml", "w") as f:
        yaml.dump(user_cfg, f)


if __name__ == "__main__":
    create()
