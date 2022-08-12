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

from pathlib import Path

import pandas as pd
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict, writeOut


class FlexEstimator:
    def __init__(self, configDict: dict, datasetID: str, activities: pd.DataFrame):
        self.datasetID = datasetID
        self.flexConfig = configDict['flexConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.upperBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Maximum_SOC']
        self.lowerBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Minimum_SOC']
        self.activities = activities.copy()
        self.isTrip = self.activities['tripID'].fillna(0).astype(bool)
        self.isPark = self.activities['parkID'].fillna(0).astype(bool)
        self.isFirstAct = self.activities['isFirstActivity'].fillna(0).astype(bool)
        self.isLastAct = self.activities['isLastActivity'].fillna(0).astype(bool)
        self.activities[['maxBatteryLevelStart', 'maxBatteryLevelEnd', 'minBatteryLevelStart',
                         'minBatteryLevelEnd', 'maxBatteryLevelEnd_unlimited', 'minBatteryLevelEnd_unlimited',
                         'maxResidualNeed', 'minResidualNeed', 'maxOvershoot', 'minOvershoot']] = None

    def _drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption'] / 100

    def _maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark, 'maxChargeVolume'] = self.activities.loc[self.isPark, 'chargingPower'] * \
            self.activities.loc[self.isPark, 'timedelta'] / pd.Timedelta('1 hour')

    # @profile(immediate=True)
    def _batteryLevelMax(self):
        """
        Calculate the maximum battery level at the beginning and end of each activity. This represents the case of
        vehicle users always connecting when charging is available and charging as soon as possible as fast as possible
        until the maximum battery capacity is reached.
        actTemp is the overall collector for each activity's park and trip results, that will then get written to
        self.activities at the very end.
        """
        print('Starting maximum battery level calculation')
        firstActs = self._calcMaxBatFirstAct()
        firstParkActs = firstActs.loc[~firstActs['parkID'].isna(), :]

        # The second condition is needed to circumvent duplicates with tripIDs=1 which are initiated above
        firstTripActs = firstActs.loc[(~firstActs['tripID'].isna()) & (firstActs['isFirstActivity']), :]
        actTemp = pd.concat([firstParkActs, firstTripActs])

        # Start and end for all trips and parkings in between
        setActs = range(self.activities['parkID'].max() + 1)
        tripActsRes = pd.DataFrame()
        for act in setActs:
            print(f'Calculate maximum battery level for act {act}')
            tripRows = (self.activities['tripID'] == act) & (~self.activities['isFirstActivity'])
            parkRows = (self.activities['parkID'] == act) & (~self.activities['isFirstActivity'])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]

            # Filtering for the previous trips that have the current activity as next activity
            prevTripActs = actTemp.loc[(actTemp['nextActID'] == act) & (~actTemp['tripID'].isna()), :]

            if act == 1:  # trips with tripID==0 (overnight morning splits) are handled in firstAct above
                tripActsRes = self._calcBatLevTripMax(actID=act, tripActs=tripActs, prevParkActs=firstParkActs)

            elif act != 0:
                # Park activities start off a new activity index e.g. parkAct 1 is always before tripAct 1
                parkActsRes = self._calcBatLevParkMax(actID=act, parkActs=parkActs, prevTripActs=prevTripActs)
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)

                prevParkActs = actTemp.loc[(actTemp['nextActID'] == act) & (~actTemp['parkID'].isna()), :]
                tripActsRes = self._calcBatLevTripMax(actID=act, tripActs=tripActs, prevParkActs=prevParkActs)

            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            prevTripActs = tripActsRes  # Redundant?
        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'])

    # @profile(immediate=True)
    def _batteryLevelMin(self):
        """
        Calculate the minimum battery level at the beginning and end of each activity. This represents the case of
        vehicles just being charged for the energy required for the next trip and as late as possible. The loop works
        exactly inverted to the batteryLevelMax() function since later trips influence the energy that has to be
        charged in parking activities before. Thus, activities are looped over from the last activity to first.
        """

        print('Starting minimum battery level calculation')
        lastActs = self._calcMinBatLastAct()
        actTemp = lastActs

        # Start and end for all trips and parkings starting from the last activities, then looping to earlier acts
        setActs = range(self.activities['parkID'].max() - 1, -1, -1)
        for act in setActs:
            print(f'Calculate minimum battery level for act {act}')
            tripRows = (self.activities['tripID'] == act) & (~self.activities['isLastActivity'])
            parkRows = (self.activities['parkID'] == act) & (~self.activities['isLastActivity'])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            nextParkActs = actTemp.loc[~actTemp['parkID'].isna(), :]
            # if act == self.activities['parkID'].max() - 1:
            #    tripActsRes = self.calcBatLevTripMin(actID=act, tripActs=tripActs, nextParkActs=lastActs)
            #    actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            #    nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            #    parkActsRes = self.calcBatLevParkMin(actID=act, parkActs=parkActs, nextTripActs=nextTripActs)
            # else:
            tripActsRes = self._calcBatLevTripMin(actID=act, tripActs=tripActs, nextParkActs=nextParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            parkActsRes = self._calcBatLevParkMin(actID=act, parkActs=parkActs, nextTripActs=nextTripActs)
            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'], ignore_index=True)

    def _calcMaxBatFirstAct(self):
        # First activities - parking and trips
        idx = (self.activities['isFirstActivity']) | (self.activities['parkID'] == 1)
        firstAct = self.activities.loc[idx, :].copy()
        firstAct.loc[:, 'maxBatteryLevelStart'] = self.upperBatLev
        firstAct.loc[self.isPark, 'maxBatteryLevelEnd'] = firstAct['maxBatteryLevelStart']
        firstAct.loc[self.isPark, 'maxOvershoot'] = firstAct['maxChargeVolume']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd_unlimited'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelStart'] - firstAct.loc[self.isTrip, 'drain']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        firstAct.loc[self.isTrip, 'maxResidualNeed'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        return firstAct

    def _calcMinBatLastAct(self):
        # Last activities - parking and trips
        lastAct = self.activities.loc[self.activities['isLastActivity'], :].copy()
        lastAct.loc[self.isPark, 'minBatteryLevelEnd'] = self.lowerBatLev
        lastAct.loc[self.isPark, 'minBatteryLevelStart'] = self.lowerBatLev
        lastAct.loc[self.isTrip, 'minBatteryLevelEnd'] = self.lowerBatLev
        lastAct.loc[self.isTrip, 'minBatteryLevelStart_unlimited'] = self.lowerBatLev + lastAct.loc[
            self.isTrip, 'drain']
        lastAct.loc[self.isTrip, 'minBatteryLevelStart'] = lastAct.loc[self.isTrip,
                                                                       'minBatteryLevelStart_unlimited'].where(
            lastAct.loc[self.isTrip, 'minBatteryLevelStart_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        resNeed = lastAct.loc[self.isTrip, 'minBatteryLevelStart_unlimited'] - self.upperBatLev
        lastAct.loc[self.isTrip, 'residualNeed'] = resNeed.where(resNeed >= 0, other=0)
        return lastAct

    def _calcBatLevTripMax(self, actID: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        # FIXME: Implement case without first parking activity (either via overnight or in the real surveyed trips)

        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        prevParkIDs = tripActs.loc[:, 'prevActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
        prevParkActsIdx = prevParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelStart'] = prevParkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelEnd'].values

        # Setting maximum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelStart'] - tripActsIdx.loc[multiIdxTrip, 'drain']
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tripActsIdx.loc[multiIdxTrip, 'maxResidualNeed'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        return tripActsIdx.reset_index()

    def _calcBatLevTripMin(self, actID: int, tripActs: pd.DataFrame, nextParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]
        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        nextParkIDs = tripActs.loc[:, 'nextActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, nextParkIDs)]
        nextParkActsIdx = nextParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelEnd'] = nextParkActsIdx.loc[
            multiIdxPark, 'minBatteryLevelStart'].values
        # Setting minimum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] = tripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelEnd'] + tripActsIdx.loc[multiIdxTrip, 'drain']
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart'] = tripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelStart_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'minBatteryLevelStart_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        resNeed = tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] - self.upperBatLev
        tripActsIdx.loc[multiIdxTrip, 'minResidualNeed'] = resNeed.where(resNeed >= 0, other=0)
        return tripActsIdx.reset_index()

    def _calcBatLevParkMax(self, actID: int, parkActs: pd.DataFrame, prevTripActs: pd.DataFrame = None):
        # Setting next park activity battery start level to battery end level of current trip
        activeHHPersonIDs = parkActs.loc[:, 'hhPersonID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        # Preliminary defs
        prevTripIDs = parkActs.loc[:, 'prevActID']
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs)]
        prevTripActsIdx = prevTripActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelStart'] = prevTripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd'].values
        parkActsIdx['maxBatteryLevelEnd_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelStart'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelEnd'] = parkActsIdx['maxBatteryLevelEnd_unlimited'].where(
            parkActsIdx['maxBatteryLevelEnd_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        tmpOvershoot = parkActsIdx['maxBatteryLevelEnd_unlimited'] - self.upperBatLev
        parkActsIdx['maxOvershoot'] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def _calcBatLevParkMin(self, actID: int, parkActs: pd.DataFrame, nextTripActs: pd.DataFrame = None):
        """Calculate minimum battery levels for given parking activities based on the given next trip activities.
        The calculated battery levels only suffice for the trips and thus describe a technical lower level for
        each activity. This function is called looping through the parking activities from largest to smallest.
        The column "minOvershoot" describes electricity volume that can be charged beyond the given battery
        capacity.

        Args:
            actID (int): _description_
            parkActs (pd.DataFrame): _description_
            nextTripActs (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        # Setting next park activity battery start level to battery end level of current trip
        activeHHPersonIDs = parkActs.loc[:, 'hhPersonID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        # Preliminary defs
        nextTripIDs = parkActs.loc[:, 'nextActID']
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, nextTripIDs)]
        nextTripActsIdx = nextTripActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelEnd'] = nextTripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelStart'].values
        # FIXME: This should not be oriented at maxChargeVolume but rather at drain of next trip
        parkActsIdx['minBatteryLevelStart_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'minBatteryLevelEnd'] - parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelStart'] = parkActsIdx['minBatteryLevelStart_unlimited'].where(
            parkActsIdx['minBatteryLevelStart_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tmpUndershoot = parkActsIdx['minBatteryLevelStart_unlimited'] - self.lowerBatLev
        parkActsIdx['minUndershoot'] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def _uncontrolledCharging(self):
        self.activities.loc[~self.activities['parkID'].isna(), 'uncontrolledCharge'] = self.activities[
            'maxBatteryLevelEnd'] - self.activities['maxBatteryLevelStart']

    def _auxFuelNeed(self):
        self.activities['auxiliaryFuelNeed'] = self.activities['residualNeed'] * self.flexConfig[
            'Fuel_consumption'] / self.flexConfig['Electric_consumption']

    def writeOutput(self):
        writeOut(dataset=self.activities, outputFolder='flexOutput', fileKey='outputFlexEstimator', manualLabel='',
                 datasetID=self.datasetID, localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def estimateTechnicalFlexibility(self):
        self._drain()
        self._maxChargeVolumePerParkingAct()
        self._batteryLevelMax()
        self._uncontrolledCharging()
        self._batteryLevelMin()
        print("Technical flexibility estimation ended")


if __name__ == "__main__":

    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath)

    datasetID = "MiD17"  # options are MiD08, MiD17, KiD and VF
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()
    vpFlex.writeOutput()
