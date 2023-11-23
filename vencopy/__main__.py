__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__status__ = "test"  # options are: dev, test, prod
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
    help="Specify if tutorials should be copied to the user folder on set up." "Defaults to true",
)
def create(name: str, tutorials: bool):
    """
    Folder set up after installation.


    Args:
        name (str): _description_
        tutorials (bool): _description_
    """
    cwd = pathlib.Path(os.getcwd())
    target = cwd / name
    source = pathlib.Path(vencopy.__file__).parent.resolve()
    if not os.path.exists(target):
        os.mkdir(target)
        setup_folders(src=source, trg=target, tutorials=tutorials)
        click.echo(f"venco.py user folder created under {target}")
    elif os.path.exists(target) and not os.path.exists(target / "run.py"):
        setup_folders(src=source, trg=target, tutorials=tutorials)
        click.echo(f"venco.py user folder filled under {target}")
    else:
        click.echo(
            "File run.py already exists in specified folder, for a new setup please specify a non-existent "
            "folder name"
        )


def setup_folders(src: pathlib.Path, trg: pathlib.Path, tutorials: bool):
    """
    Setup function to create a vencopy user folder and to copy run, config and tutorial files from the package source.

    Args:
        src (pathlib.Path): Absolute path to the vencopy package source folder
        trg (pathlib.Path): Absolute path to the vencopy user folder
        tutorials (bool): Boolean, if true (default) tutorials are being copied from package source to user folder
    """
    os.mkdir(trg / "config")
    os.mkdir(trg / "core")
    os.mkdir(trg / "output")
    os.mkdir(trg / "output" / "dataparser")
    os.mkdir(trg / "output" / "diarybuilder")
    os.mkdir(trg / "output" / "gridmodeller")
    os.mkdir(trg / "output" / "flexestimator")
    os.mkdir(trg / "output" / "profileaggregator")
    os.mkdir(trg / "output" / "postprocessor")
    os.mkdir(trg / "utils")
    shutil.copy(src=src / "run.py", dst=trg)
    shutil.copytree(src=src / "config", dst=trg / "config")
    if tutorials:
        raise (NotImplementedError("Tutorials for the new venco.py iteration are not yet implemented."))
        # shutil.copytree(src=src / "tutorials", dst=trg / "tutorials")
    update_config(new_vencopy_root=trg)


def update_config(new_vencopy_root: pathlib.Path):
    """
    _missing_

    Args:
        new_vencopy_root (pathlib.Path): _description_
    """
    with open(new_vencopy_root / "config" / "user.yaml") as f:
        user_cfg = yaml.load(f, Loader=yaml.SafeLoader)

    user_cfg["absolute_path"]["vencopy_root"] = new_vencopy_root.__str__()

    with open(new_vencopy_root / "config" / "user.yaml", "w") as f:
        yaml.dump(user_cfg, f)


if __name__ == "__main__":
    create()
