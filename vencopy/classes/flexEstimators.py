__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pandas as pd
from pathlib import Path
from vencopy.classes.dataParsers import ParseMiD, ParseVF, ParseKiD


class FlexEstimator:
    def __init__(
        self,
        configDict: dict,
        activityData: pd.DataFrame,
    ):
        self.flexConfig = configDict['flexConfig']
        self.nettoBatCap = self.flexConfig['Battery_capacity'] * self.flexConfig['Maximum_SOC']
        self.lowerBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Minimum_SOC']
        self.activitiesIn = activityData
        self.activities = self.activitiesIn.copy()
        self.tripIdx = self.activities.loc[self.activities['tripID'].fillna(0).astype(bool), :].index
        self.parkIdx = self.activities.loc[self.activities['parkID'].fillna(0).astype(bool), :].index

    def drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption']

    # FixME: Could not be tested yet, replace 'ratedPower' by charging station capacity column
    def maxChargeVolume(self):
        self.activities.loc[self.parkIdx, 'maxChargeVolume'] = self.activities.loc[self.parkIdx, 'ratedPower'] * \
            self.activities.loc[self.parkIdx, 'timedelta'] / pd.Timedelta('1 hour')

    def maxBatteryLevel(self):
        # First parking
        self.activities.loc[self.activities['isFirstActivity'], 'maxBatteryLevelStart'] = self.nettoBatCap
        self.activities['maxBatteryLevelEnd'] = self.activities['maxBatteryLevelStart']

        # All trips and parkings in between
        setActs = range(self.activities['parkID'].max())
        for act in setActs:
            self.calcMaxBatLevTripPark(actID=act)

        # Last parking FIXME: DO WE NEED TO DIFFERENTIATE THIS CASE?
        # self.activities.loc[self.activities['isLastActivity'], 'maxBatteryLevelStart']

    def calcMaxBatLevTripPark(self, actID: int):
        trip = self.activities.loc[self.activities['tripID'] == actID, :]
        park = self.activities.loc[self.activities['parkID'] == actID + 1, :]
        trip['maxBatteryLevelStart'] = self.activities.loc[self.activities['parkID'] == actID - 1, 'maxBatteryLevelEnd']
        trip['maxBatteryLevelEnd'] = trip['maxBatteryLevelStart'] - trip['drain']
        park['maxBatteryLevelStart'] = trip['maxBatteryLevelEnd']
        theoBatLev = park['maxBatteryLevelStart'] + park['maxChargeVolume']
        park['maxBatteryLevelEnd'] = theoBatLev.where(theoBatLev <= self.nettoBatCap, other=self.nettoBatCap)

    def estimateTechnicalFlexibility(self):
        self.drain()
        self.maxChargeVolume()


if __name__ == "__main__":
    from vencopy.scripts.globalFunctions import loadConfigDict
    basePath = Path(__file__).parent.parent
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig',
                   'evaluatorConfig')
    configDict = loadConfigDict(configNames, basePath)

    datasetID = "MiD17"  # options are MiD08, MiD17, KiD
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()
    vpFlex = FlexEstimator(configDict=configDict, activityData=vpData.activities)
    vpFlex.estimateTechnicalFlexibility()
