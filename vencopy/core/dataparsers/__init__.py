from typing import Union

from ...core.dataparsers.parseMiD import ParseMiD
from ...core.dataparsers.parseKiD import ParseKiD


def parse_data(configs: dict) -> Union[ParseMiD, ParseKiD]:
    dataset = configs["user_config"]["global"]["dataset"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD}
    return delegate[dataset](configs=configs, dataset=dataset)
