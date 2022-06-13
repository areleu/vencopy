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
from profilehooks import profile
from vencopy.core.dataParsers import ParseMiD, ParseVF, ParseKiD
from vencopy.core.gridModelers import GridModeler



class FlexEstimator:
    def __init__(
        self,
        configDict: dict,
        activities: pd.DataFrame,
    ):
        self.flexConfig = configDict['flexConfig']
        self.upperBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Maximum_SOC']
        self.lowerBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Minimum_SOC']
        self.activities = activities.copy()
        self.isTrip = self.activities['tripID'].fillna(0).astype(bool)
        self.isPark = self.activities['parkID'].fillna(0).astype(bool)
        self.isFirstAct = self.activities['isFirstActivity'].fillna(0).astype(bool)
        self.isLastAct = self.activities['isLastActivity'].fillna(0).astype(bool)
        self.addNextAndPrevIDs()
        self.activities[['maxBatteryLevelStart', 'maxBatteryLevelEnd', 'minBatteryLevelStart',
                         'minBatteryLevelEnd', 'maxBatteryLevelEnd_unlimited', 'minBatteryLevelEnd_unlimited',
                         'maxResidualNeed', 'minResidualNeed', 'maxOvershoot', 'minOvershoot']] = None

    def addNextAndPrevIDs(self):
        self.activities.loc[~self.activities['tripID'].isna(), 'actID'] = self.activities['tripID']
        self.activities.loc[~self.activities['parkID'].isna(), 'actID'] = self.activities['parkID']
        self.activities.loc[~self.activities['isLastActivity'], 'nextActID'] = self.activities.loc[
            :, 'actID'].shift(-1)
        self.activities.loc[~self.activities['isFirstActivity'], 'prevActID'] = self.activities.loc[
            :, 'actID'].shift(1)

    def drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption'] / 100

    def maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark, 'maxChargeVolume'] = self.activities.loc[self.isPark, 'chargingPower'] * \
            self.activities.loc[self.isPark, 'timedelta'] / pd.Timedelta('1 hour')

    @profile(immediate=True)
    def batteryLevelMax(self):
        """
        Calculate the maximum battery level at the beginning and end of each activity. This represents the case of
        vehicle users always connecting when charging is available and charging as soon as possible as fast as possible
        until the maximum battery capacity is reached.
        """

        # SUPPOSEDLY DEPRECATED GENERALIZED APPROACH
        # Calculate max battery start and end of all first activities
        # if type == 'max':
        #     firstActs = self.calcMaxBatFirstAct()
        # elif type == 'min':
        #     firstActs = self.calcMinBatFirstAct()
        # else:
        #     raise(ValueError(f'Specified value {type} not allowed, please select "min" or "max".'))

        print('Starting maximum battery level calculation')
        firstActs = self.calcMaxBatFirstAct()
        actTemp = firstActs

        # Start and end for all trips and parkings in between
        setActs = range(1, self.activities['parkID'].max() + 1)
        for act in setActs:
            print(f'Calculate maximum battery level for act {act}')
            tripRows = (self.activities['tripID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct  probably not needed
            parkRows = (self.activities['parkID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            prevTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]

            if act == 1:
                tripActsRes = self.calcBatLevTripMax(actID=act, tripActs=tripActs, prevParkActs=firstActs)
            else:
                parkActsRes = self.calcBatLevParkMax(actID=act, parkActs=parkActs, prevTripActs=prevTripActs)
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
                prevParkActs = actTemp.loc[~actTemp['parkID'].isna(), :]
                tripActsRes = self.calcBatLevTripMax(actID=act, tripActs=tripActs, prevParkActs=prevParkActs)

            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            prevTripActs = tripActsRes  # FIXME: Is this line redundant b/c of line 93?

        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'])

    @profile(immediate=True)
    def batteryLevelMin(self):
        """
        Calculate the minimum battery level at the beginning and end of each activity. This represents the case of
        vehicles just being charged for the energy required for the next trip and as late as possible. The loop works
        exactly inverted to the batteryLevelMax() function since later trips influence the energy that has to be
        charged in parking activities before. Thus, activities are looped over from the last activity to first.
        """
        print('Starting minimum battery level calculation')
        lastActs = self.calcMinBatLastAct()
        actTemp = lastActs

        # Start and end for all trips and parkings starting from the last activities, then looping to earlier acts
        setActs = range(self.activities['parkID'].max() - 1, 0, -1)
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
            tripActsRes = self.calcBatLevTripMin(actID=act, tripActs=tripActs, nextParkActs=nextParkActs)

            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            parkActsRes = self.calcBatLevParkMin(actID=act, parkActs=parkActs, nextTripActs=nextTripActs)
            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)

        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'], ignore_index=True)

    def calcMaxBatFirstAct(self):
        # First activities - parking and trips
        firstAct = self.activities.loc[self.isFirstAct, :]
        firstAct.loc[:, 'maxBatteryLevelStart'] = self.upperBatLev
        firstAct.loc[self.isPark, 'maxBatteryLevelEnd'] = firstAct['maxBatteryLevelStart']
        firstAct.loc[self.isPark, 'overshoot'] = firstAct['maxChargeVolume']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd_unlimited'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelStart'] - firstAct.loc[self.isTrip, 'drain']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        firstAct.loc[self.isTrip, 'residualNeed'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        return firstAct

    def calcMinBatLastAct(self):
        # Last activities - parking and trips
        lastAct = self.activities.loc[self.activities['isLastActivity'], :]
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

    def calcBatLevTripMax(self, actID: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]

        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        prevParkIDs = tripActs.loc[:, 'prevActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
        prevParkActsIdx = prevParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelStart'] = prevParkActsIdx.loc[multiIdxPark,
                                                                                    'maxBatteryLevelEnd'].values
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

    def calcBatLevTripMin(self, actID: int, tripActs: pd.DataFrame, nextParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]

        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        nextParkIDs = tripActs.loc[:, 'nextActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, nextParkIDs)]
        nextParkActsIdx = nextParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelEnd'] = nextParkActsIdx.loc[multiIdxPark,
                                                                                  'minBatteryLevelStart'].values

        # Setting minimum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] = tripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelEnd'] + tripActsIdx.loc[multiIdxTrip, 'drain']
        tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart'] = tripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelStart_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'minBatteryLevelStart_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        resNeed = tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] - self.upperBatLev
        tripActsIdx.loc[multiIdxTrip, 'minResidualNeed'] = resNeed.where(resNeed >= 0, other=0)
        return tripActsIdx.reset_index()

    def calcBatLevParkMax(self, actID: int, parkActs: pd.DataFrame, prevTripActs: pd.DataFrame = None):
        # Setting next park activity battery start level to battery end level of current trip
        activeHHPersonIDs = parkActs.loc[:, 'hhPersonID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        # Preliminary defs
        prevTripIDs = parkActs.loc[:, 'prevActID']
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs)]
        prevTripActsIdx = prevTripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelStart'] = prevTripActsIdx.loc[multiIdxTrip,
                                                                                    'maxBatteryLevelEnd'].values

        parkActsIdx['maxBatteryLevelEnd_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelStart'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelEnd'] = parkActsIdx['maxBatteryLevelEnd_unlimited'].where(
            parkActsIdx['maxBatteryLevelEnd_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        tmpOvershoot = parkActsIdx['maxBatteryLevelEnd_unlimited'] - self.upperBatLev
        parkActsIdx['maxOvershoot'] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def calcBatLevParkMin(self, actID: int, parkActs: pd.DataFrame, nextTripActs: pd.DataFrame = None):
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

        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelEnd'] = nextTripActsIdx.loc[multiIdxTrip,
                                                                                  'minBatteryLevelStart'].values

        # FIXME: This should not be oriented at maxChargeVolume but rather at drain of next trip
        parkActsIdx['minBatteryLevelStart_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'minBatteryLevelEnd'] - parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']

        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelStart'] = parkActsIdx['minBatteryLevelStart_unlimited'].where(
            parkActsIdx['minBatteryLevelStart_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tmpUndershoot = parkActsIdx['minBatteryLevelStart_unlimited'] - self.lowerBatLev
        parkActsIdx['minUndershoot'] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)

        return parkActsIdx.reset_index()

    # DEPRECATED
    def calcMaxBatLastAct(self):
        """Calculate maximum battery levels for all last activities be it parking or trips
        """
        lastTripIdx = (self.isTrip) & (self.isLastAct)
        lastTripIdx = lastTripIdx.loc[lastTripIdx].index
        self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] = self.activities.loc[lastTripIdx - 1,
                                                                                       'maxBatteryLevelEnd']
        theoBatLev = self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] - self.activities.loc[lastTripIdx,
                                                                                                    'drain']
        self.activities.loc[lastTripIdx, 'maxBatteryLevelEnd'] = theoBatLev.where(theoBatLev >= 0, other=0)

    def uncontrolledCharging(self):
        self.activities.loc[~self.activities['parkID'].isna(), 'uncontrolledCharge'] = self.activities[
            'maxBatteryLevelEnd'] - self.activities['maxBatteryLevelStart']

    def auxFuelNeed(self):
        self.activities['auxiliaryFuelNeed'] = self.activities['residualNeed'] * self.flexConfig[
            'Fuel_consumption'] / self.flexConfig['Electric_consumption']

    def estimateTechnicalFlexibility(self):
        self.drain()
        self.maxChargeVolumePerParkingAct()
        self.batteryLevelMax()
        self.uncontrolledCharging()
        self.batteryLevelMin()
        print("Technical flexibility estimation ended")

if __name__ == "__main__":
    from vencopy.utils.globalFunctions import loadConfigDict

    basePath = Path(__file__).parent.parent
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'diaryConfig', 'gridConfig', 'flexConfig',
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

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()
