__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '29.07.2022'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import numpy as np
import pandas as pd

from pathlib import Path
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.flexEstimators import DiaryBuilder
from vencopy.utils.globalFunctions import loadConfigDict  # , writeOut


class ProfileAggregator():
    def __init__(self, configDict: dict, activities: pd.DataFrame, profiles: list):
        self.aggregatorConfig = configDict['aggregatorConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.profiles = profiles
        self.nClusters = self.aggregatorConfig['clustering']['nClusters']

    def pickProfiles(self):
        # read from disk or from previous class
        data_dir = ''
        self.drain = pd.read_csv(data_dir + '', index_col='Unnamed: 1')
        # self.drain = vpDiary.drain

    def cluster(self):
        pass

    def createWeeklyProfiles(self):
        pass

    def createAnnualProfiles(self):
        pass

    def createProfile(self):
        self.readProfiles()
        self.cluster()
        self.createWeeklyProfiles()
        self.createAnnualProfiles()

    def createTimeseries(self):
        self.drain = self.drain.createProfile(column="drain")
        # self.uncontrolledCharge = self.uncontrolledCharge.createProfile(column="uncontrolledCharge")
        # self.residualNeed = self.residualNeed.createProfile(column="residualNeed")
        # self.maxBatteryLevel = self.maxBatteryLevel.createProfile(column="maxBatteryLevel")
        # self.minBatteryLevel = self.minBatteryLevel.createProfile(column="minBatteryLevel")


if __name__ == '__main__':

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpFlex.activities)
    vpDiary.createDiaries()

    profiles = ("drain")
    vpProfile = ProfileAggregator(configDict=configDict, activities=vpFlex.activities, profiles=profiles)
    vpProfile.createProfiles()
