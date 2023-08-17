


from typing import Union

from vencopy.core.dataParsers.parseMiD import ParseMiD
from vencopy.core.dataParsers.parseKiD import ParseKiD
from vencopy.core.dataParsers.parseVF import ParseVF

def parseData(configDict: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    datasetID = configDict["user_config"]["global"]["dataset"]
    debug = configDict["user_config"]["global"]["debug"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[datasetID](configDict=configDict, datasetID=datasetID, debug=debug)