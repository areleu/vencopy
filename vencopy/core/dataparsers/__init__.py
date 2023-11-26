from typing import Union

from ...core.dataparsers.parseMiD import ParseMiD
from ...core.dataparsers.parseKiD import ParseKiD
from ...core.dataparsers.parseVF import ParseVF


def parse_data(configs: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    """
    _summary_

    Args:
        configs (dict): _description_

    Returns:
        Union[ParseMiD, ParseKiD, ParseVF]: _description_
    """
    dataset = configs["user_config"]["global"]["dataset"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[dataset](configs=configs, dataset=dataset)
