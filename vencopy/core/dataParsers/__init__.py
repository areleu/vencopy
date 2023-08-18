from typing import Union

from vencopy.core.dataParsers.parseMiD import ParseMiD
from vencopy.core.dataParsers.parseKiD import ParseKiD
from vencopy.core.dataParsers.parseVF import ParseVF


def parse_data(config_dict: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    dataset_ID = config_dict["user_config"]["global"]["dataset"]
    debug = config_dict["user_config"]["global"]["debug"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[dataset_ID](config_dict=config_dict, dataset_ID=dataset_ID, debug=debug)
