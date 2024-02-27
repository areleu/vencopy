__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__license__ = "BSD-3-Clause"

import pandas as pd
import yaml
from pathlib import Path
import os


def load_configs(base_path: Path) -> dict:
    """
    Generic function to load and open yaml config files.
    Uses pathlib syntax for windows, max, linux compatibility,
    see https://realpython.com/python-pathlib/ for an introduction.

    Args:
        base_path (Path): _description_

    Returns:
        configs (dict): Dictionary with opened yaml config files
    """
    config_names = ("user_config", "dev_config")
    config_path = Path(base_path) / "config"
    configs = {}
    for config_name in config_names:
        file_path = (config_path / config_name).with_suffix(".yaml")
        with open(file_path) as ipf:
            configs[config_name] = yaml.load(ipf, Loader=yaml.SafeLoader)
    return configs


def return_lowest_level_dict_keys(dictionary: dict, lst: list = None) -> list:
    """
    Returns the lowest level keys of dictionary and returns all of them
    as a list. The parameter lst is used as
    interface between recursion levels.

    Args:
        dictionary (dict): Dictionary of variables
        lst (list, optional): List used as interface between recursion levels. Defaults to None.

    Returns:
        list: list with all the bottom level dictionary keys
    """
    if lst is None:
        lst = []
    for i_key, i_value in dictionary.items():
        if isinstance(i_value, dict):
            lst = return_lowest_level_dict_keys(i_value, lst)
        elif i_value is not None:
            lst.append(i_key)
    return lst


def return_lowest_level_dict_values(dictionary: dict, lst: list = None) -> list:
    """
    Returns a list of all dictionary values of the last dictionary level
    (the bottom) of dictionary. The parameter
    lst is used as an interface between recursion levels.

    Args:
        dictionary (dict): Dictionary of variables
        lst (list, optional): List used as interface to next recursion. Defaults to None.

    Returns:
        list: List with all the bottom dictionary values
    """
    if lst is None:
        lst = []
    for _, i_value in dictionary.items():
        if isinstance(i_value, dict):
            lst = return_lowest_level_dict_values(i_value, lst)
        elif i_value is not None:
            lst.append(i_value)
    return lst


def replace_vec(series, year=None, month=None, day=None, hour=None, minute=None, second=None) -> pd.Series:
    """
    _summary_

    Args:
        series (_type_): _description_
        year (_type_, optional): _description_. Defaults to None.
        month (_type_, optional): _description_. Defaults to None.
        day (_type_, optional): _description_. Defaults to None.
        hour (_type_, optional): _description_. Defaults to None.
        minute (_type_, optional): _description_. Defaults to None.
        second (_type_, optional): _description_. Defaults to None.

    Returns:
        pd.Series: _description_
    """
    replacement = pd.to_datetime(
        {
            "year": series.dt.year if year is None else [year for i in range(len(series))],
            "month": series.dt.month if month is None else [month for i in range(len(series))],
            "day": series.dt.day if day is None else [day for i in range(len(series))],
            "hour": series.dt.hour if hour is None else [hour for i in range(len(series))],
            "minute": series.dt.minute if minute is None else [minute for i in range(len(series))],
            "second": series.dt.second if second is None else [second for i in range(len(series))],
        })
    return replacement


def create_output_folders(configs: dict):
    """
    Function to crete vencopy output folder and subfolders

    Args:
        configs (dict): _description_
    """
    root = Path(configs["user_config"]["global"]["absolute_path"]["vencopy_root"])
    main_dir = "output"
    if not os.path.exists(Path(root / main_dir)):
        os.mkdir(Path(root / main_dir))
    sub_dirs = ("dataparser", "diarybuilder", "gridmodeller", "flexestimator", "profileaggregator", "postprocessor")
    for sub_dir in sub_dirs:
        if not os.path.exists(Path(root / main_dir / sub_dir)):
            os.mkdir(Path(root / main_dir / sub_dir))


def create_file_name(
    dev_config: dict, user_config: dict, file_name_id: str, dataset: str, suffix: str = "csv"
) -> str:
    """
    Generic method used for fileString compilation throughout the venco.py framework. This method does not write any
    files but just creates the file name including the filetype suffix.

    Args:
        dev_config (dict): _description_
        user_config (dict): _description_
        file_name_id (str): ID of respective data file as specified in global config
        dataset (str): Dataset
        manual_label (str, optional):  Optional manual label to add to file_name. Defaults to "".
        suffix (str, optional): _description_. Defaults to "csv".

    Returns:
        str: Full name of file to be written.
    """
    run_label = user_config['global']['run_label']
    if dataset is None:
        return f"{dev_config['global']['disk_file_names'][file_name_id]}_{run_label}.{suffix}"
    if len(run_label) == 0:
        return f"{dev_config['global']['disk_file_names'][file_name_id]}_{dataset}.{suffix}"
    return f"{dev_config['global']['disk_file_names'][file_name_id]}_{run_label}_{dataset}.{suffix}"


def write_out(data: pd.DataFrame, path: Path):
    """
    _summary_

    Args:
        data (pd.DataFrame): _description_
        path (Path): _description_
    """
    data.to_csv(path)
    print(f"Dataset written to {path}.")

