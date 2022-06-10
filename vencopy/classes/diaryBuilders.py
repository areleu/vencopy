__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.04.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pandas as pd
from pathlib import Path
from vencopy.scripts.globalFunctions import loadConfigDict
from vencopy.classes.dataParsers import ParseMiD, ParseKiD, ParseVF


class DiaryBuilder:
    def __init__(self, configDict: dict, activities: pd.DataFrame, debug: bool = False):
        self.tripConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        if debug:
            self.activties = self.activties.loc[0:2000, :]
        else:
            self.activties = activities.data
        # self.activties = TimeDiscretiser(self.activties)
        self.activties = self.mergeTrips()

    def mergeTrips(self):
        """
        Merge multiple individual trips into one diary consisting of multiple trips

        :param activities: Input trip data with specified time resolution
        :return: Merged trips diaries
        """
        print("Merging trips")
        # dataDay = self.activities.groupby(['genericID']).sum()
        # dataDay = self.activities.drop('tripID', axis=1)
        # return dataDay


if __name__ == '__main__':

    from vencopy.scripts.globalFunctions import loadConfigDict

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "diaryConfig",
        "gridConfig",
        "flexConfig",
        "evaluatorConfig",
    )
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpData, debug=False)
