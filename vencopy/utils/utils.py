__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2021"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pandas as pd
import yaml
from pathlib import Path
import os


def load_configs(base_path: Path) -> dict:
    """
    Generic function to load and open yaml config files.
    pathLib syntax for windows, max, linux compatibility,
    see https://realpython.com/python-pathlib/ for an introduction.

    :param config_names: Tuple containing names of config files to be loaded
    :return: Dictionary with opened yaml config files
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

    :param dictionary: Dictionary of variables
    :param lst: empty list, used as interface between recursion levels
    :return: Returns a list with all the bottom level dictionary keys
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

    :param dictionary: Dictionary of variables
    :param lst: empty list, is used as interface to next recursion
    :return: Returns a list with all the bottom dictionary values
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
    return pd.to_datetime(
        {
            "year": series.dt.year if year is None else [year for i in range(len(series))],
            "month": series.dt.month if month is None else [month for i in range(len(series))],
            "day": series.dt.day if day is None else [day for i in range(len(series))],
            "hour": series.dt.hour if hour is None else [hour for i in range(len(series))],
            "minute": series.dt.minute if minute is None else [minute for i in range(len(series))],
            "second": series.dt.second if second is None else [second for i in range(len(series))],
        }
    )


def create_output_folders(configs: dict):
    """
    Function to crete vencopy output folder and subfolders

    :param: config dictionary
    :return: None
    """
    root = Path(configs["user_config"]["global"]["absolute_path"]["vencopy_root"])
    main_dir = "output"
    if not os.path.exists(Path(root / main_dir)):
        os.mkdir(Path(root / main_dir))
    sub_dirs = ("dataparser", "diarybuilder", "gridmodeler", "flexestimator", "profileaggregator", "postprocessor")
    for sub_dir in sub_dirs:
        if not os.path.exists(Path(root / main_dir / sub_dir)):
            os.mkdir(Path(root / main_dir / sub_dir))


def create_file_name(
    dev_config: dict, user_config: dict, file_name_id: str, dataset: str, manual_label: str = "", suffix: str = "csv"
) -> str:
    """
    Generic method used for fileString compilation throughout the venco.py framework. This method does not write any
    files but just creates the file name including the filetype suffix.

    :param user_config: user config file for paths
    :param file_name_id: ID of respective data file as specified in global config
    :param dataset: Manual specification of data set ID e.g. 'MiD17'
    :param manual_label: Optional manual label to add to file_name
    :param filetypeStr: filetype to be written to hard disk
    :return: Full name of file to be written.
    """
    if dataset is None:
        return f"{dev_config['global']['disk_file_names'][file_name_id]}_{user_config['global']['run_label']}_{manual_label}.{suffix}"
    if len(manual_label) == 0:
        return f"{dev_config['global']['disk_file_names'][file_name_id]}_{user_config['global']['run_label']}_{dataset}.{suffix}"
    return f"{dev_config['global']['disk_file_names'][file_name_id]}_{user_config['global']['run_label']}_{manual_label}_{dataset}.{suffix}"


def write_out(data: pd.DataFrame, path: Path):
    data.to_csv(path)
    print(f"Dataset written to {path}.")

