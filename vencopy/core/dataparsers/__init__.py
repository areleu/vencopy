from typing import Union

from vencopy.core.dataparsers.parseMiD import ParseMiD
from vencopy.core.dataparsers.parseKiD import ParseKiD
from vencopy.core.dataparsers.parseVF import ParseVF


def parse_data(configs: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    dataset = configs["user_config"]["global"]["dataset"]
    debug = configs["user_config"]["global"]["debug"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[dataset](configs=configs, dataset=dataset, debug=debug)
