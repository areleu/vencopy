from typing import Union

from ...core.dataparsers.parseMiD import ParseMiD
from ...core.dataparsers.parseKiD import ParseKiD
from ...core.dataparsers.parseVF import ParseVF


def parse_data(configs: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    """
    Wrapper function to process the instatiated DataParser class. The function calls the children classes which are dataset specific.

    Args:
        configs (dict): A dictionary containing a user_config dictionary and a dev_config dictionary.

    Returns:
        Union[ParseMiD, ParseKiD, ParseVF]: Children classes of the DataParser class to parse specific mobility datasets.
    """
    dataset = configs["user_config"]["global"]["dataset"]
    delegate = {"MiD08": ParseMiD, "MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[dataset](configs=configs, dataset=dataset)
