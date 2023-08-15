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
from profilehooks import profile

import pandas as pd
from vencopy.utils.globalFunctions import createFileName, writeOut


class FlexEstimator:
    def __init__(self, configDict: dict, activities: pd.DataFrame):
        self.datasetID = configDict["globalConfig"]["dataset"]
        self.flexConfig = configDict['flexConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.upperBatLev = self.flexConfig[
            'Battery_capacity'] * self.flexConfig['Maximum_SOC']
        self.lowerBatLev = self.flexConfig[
            'Battery_capacity'] * self.flexConfig['Minimum_SOC']
        self.activities = activities.copy()
        self.isTrip = ~self.activities['tripID'].isna()
        self.isPark = ~self.activities['parkID'].isna()
        self.isFirstAct = self.activities['isFirstActivity'].fillna(
            0).astype(bool)
        self.isLastAct = self.activities['isLastActivity'].fillna(
            0).astype(bool)

        # UC = uncontrolled charging
        self.activities[['maxBatteryLevelStart', 'maxBatteryLevelEnd',
                         'minBatteryLevelStart', 'minBatteryLevelEnd',
                         'maxBatteryLevelEnd_unlimited', 'uncontrolledCharge',
                         'timestampEndUC_unltd', 'timestampEndUC',
                         'minBatteryLevelEnd_unlimited', 'maxResidualNeed',
                         'minResidualNeed', 'maxOvershoot', 'minUndershoot',
                         'auxiliaryFuelNeed']] = None
        self.activitiesWOResidual = None

    def _drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption'] / 100

    def _maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark,
                            'maxChargeVolume'] = self.activities.loc[
            self.isPark, 'availablePower'] * self.activities.loc[
                self.isPark, 'timedelta'] / pd.Timedelta('1 hour')

    # @profile(immediate=False)
    def __batteryLevelMax(self, startLevel: float):
        """
        Calculate the maximum battery level at the beginning and end of each
        activity. This represents the case of vehicle users always connecting
        when charging is available and charging as soon as possible as fast as
        possible until the maximum battery capacity is reached. actTemp is the
        overall collector for each activity's park and trip results, that will
        then get written to self.activities at the very end.

        Args:
            startLevel (float): Battery start level for first activity of the
            activity chain
        """
        print('Starting maximum battery level calculation.')
        firstActs = self._calcMaxBatFirstAct(startLevel=startLevel)
        firstParkActs = firstActs.loc[~firstActs['parkID'].isna(), :]
        # The second condition is needed to circumvent duplicates with tripIDs=1
        # which are initiated above
        firstTripActs = firstActs.loc[(~firstActs['tripID'].isna()) & (
            firstActs['isFirstActivity']), :]
        actTemp = pd.concat([firstParkActs, firstTripActs])
        # Start and end for all trips and parkings in between
        setActs = range(int(self.activities['parkID'].max()) + 1)
        tripActsRes = pd.DataFrame()  # Redundant?
        for act in setActs:  # implementable via groupby with actIDs as groups?
            print(f'Calculating maximum battery level for act {act}.')
            tripRows = (self.activities['tripID'] == act) & (
                ~self.activities['isFirstActivity'])
            parkRows = (self.activities['parkID'] == act) & (
                ~self.activities['isFirstActivity'])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            # Filtering for the previous trips that have the current activity as
            # next activity
            prevTripActs = actTemp.loc[
                (actTemp['nextActID'] == act) & (~actTemp['tripID'].isna()), :]
            # firstAct trips with tripID==0 (overnight morning splits) are
            # handled in _calcMaxBatFirstAct above
            if act == 1:
                tripActsRes = self.__calcBatLevTripMax(
                    actID=act, tripActs=tripActs, prevParkActs=firstParkActs)
            elif act != 0:
                # Park activities start off a new activity index e.g. parkAct 1 is always before tripAct 1
                parkActsRes = self.__calcBatLevParkMax(
                    actID=act, parkActs=parkActs, prevTripActs=prevTripActs)
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
                prevParkActs = actTemp.loc[(actTemp['nextActID'] == act) & (
                    ~actTemp['parkID'].isna()), :]
                tripActsRes = self.__calcBatLevTripMax(
                    actID=act, tripActs=tripActs, prevParkActs=prevParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            prevTripActs = tripActsRes  # Redundant?
        self.activities = actTemp.sort_values(
            by=['uniqueID', 'actID', 'parkID'])
        return self.activities.loc[
            self.activities['isLastActivity'], [
                'uniqueID', 'maxBatteryLevelEnd']].set_index('uniqueID')

    # @profile(immediate=False)
    def __batteryLevelMin(self, endLevel: pd.Series):
        """
        Calculate the minimum battery level at the beginning and end of each
        activity. This represents the case of vehicles just being charged for
        the energy required for the next trip and as late as possible. The loop
        works exactly inverted to the batteryLevelMax() function since later
        trips influence the energy that has to be charged in parking activities
        before. Thus, activities are looped over from the last activity to
        first.
        """
        print('Starting minimum battery level calculation.')
        print(f'Calculate minimum battery level for act {int(self.activities.actID.max())}.')
        lastActs = self._calcMinBatLastAct(endLevel=endLevel)
        actTemp = lastActs
        # Start and end for all trips and parkings starting from the last
        # activities, then looping to earlier acts
        setActs = range(int(self.activities['parkID'].max()) - 1, -1, -1)
        for act in setActs:
            print(f'Calculate minimum battery level for act {act}.')
            tripRows = (self.activities['tripID'] == act) & (
                ~self.activities['isLastActivity'])
            parkRows = (self.activities['parkID'] == act) & (
                ~self.activities['isLastActivity'])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            nextParkActs = actTemp.loc[~actTemp['parkID'].isna(), :]

            tripActsRes = self.__calcBatLevTripMin(
                actID=act, tripActs=tripActs, nextParkActs=nextParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            parkActsRes = self.__calcBatLevParkMin(
                actID=act, parkActs=parkActs, nextTripActs=nextTripActs)
            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
        self.activities = actTemp.sort_values(
            by=['uniqueID', 'actID', 'parkID'], ignore_index=True)
        return self.activities.loc[
            self.activities['isFirstActivity'], [
                'uniqueID', 'minBatteryLevelStart']].set_index('uniqueID')

    def _calcMaxBatFirstAct(self, startLevel: float) -> pd.DataFrame:
        """
        Calculate maximum battery levels at beginning and end of the first activities. If overnight trips are split
        up, not only first activities are being treated (see details in docstring of self._getFirstActIdx())

        Args:
            startLevel (float): Start battery level at beginning of simulation (MON, 00:00). Defaults to
            self.upperBatLev, the maximum battery level.
        Returns:
            pd.DataFrame: First activities with all battery level columns as anchor for the consecutive calculation
            of maximum charge
        """
        # First activities - parking and trips
        idx = self.__getFirstActIdx()
        firstAct = self.activities.loc[idx, :].copy()
        fa = firstAct.set_index('uniqueID')
        fa['maxBatteryLevelStart'] = startLevel
        fa = fa.reset_index('uniqueID')
        fa.index = firstAct.index
        firstAct = fa
        isPark = ~firstAct['parkID'].isna()
        isTrip = ~firstAct['tripID'].isna()
        firstAct.loc[isPark, 'maxBatteryLevelEnd_unlimited'] = firstAct['maxBatteryLevelStart'] + firstAct[
            'maxChargeVolume']
        firstAct.loc[isPark, 'maxBatteryLevelEnd'] = firstAct.loc[isPark, 'maxBatteryLevelEnd_unlimited'].where(
            firstAct.loc[isPark, 'maxBatteryLevelEnd_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        firstAct.loc[isPark, 'maxOvershoot'] = firstAct['maxBatteryLevelEnd_unlimited'] - firstAct['maxBatteryLevelEnd']
        firstAct.loc[isTrip, 'maxBatteryLevelEnd_unlimited'] = firstAct.loc[
            isTrip, 'maxBatteryLevelStart'] - firstAct.loc[isTrip, 'drain']
        firstAct.loc[isTrip, 'maxBatteryLevelEnd'] = firstAct.loc[isTrip, 'maxBatteryLevelEnd_unlimited'].where(
            firstAct.loc[isTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        res = firstAct.loc[isTrip, 'maxBatteryLevelEnd'] - firstAct.loc[isTrip, 'maxBatteryLevelEnd_unlimited']
        firstAct.loc[isTrip, 'maxResidualNeed'] = res.where(firstAct.loc[
            isTrip, 'maxBatteryLevelEnd_unlimited'] < self.lowerBatLev, other=0)
        return firstAct

    def __getFirstActIdx(self) -> pd.Series:
        """ Get indices of all activities that should be treated here. These comprise not only the first activities
        determined by the column isFirstActivity but also the split-up overnight trips with the tripID==0 and the first
        parking activities with parkID==1. This method is overwritten in the FlexEstimatorWeek.

        Returns:
            pd.Series: Boolean Series identifying the relevant rows for calculating SOCs for first activities
        """
        return (self.activities['isFirstActivity']) | (self.activities['parkID'] == 1)

    def _calcMinBatLastAct(self, endLevel) -> pd.DataFrame:
        """Calculate the minimum battery levels for the last activity in the data set determined by the maximum activity
        ID.

        Args:
            endLevel (float or pd.Series): End battery level at end of simulation time (lastBin). Defaults to
            self.lowerBatLev, the minimum battery level. Can be either of type float (in first iteration) or pd.Series
            with respective uniqueID in the index.
        Returns:
            pd.DataFrame: Activity data set with the battery variables set for all last activities of the activity
            chains
        """
        # Last activities - parking and trips
        lastActIn = self.activities.loc[self.activities['isLastActivity'], :].copy()
        isTrip = ~lastActIn['tripID'].isna()

        lastActIdx = lastActIn.set_index('uniqueID')
        lastActIdx['minBatteryLevelEnd'] = endLevel
        lastActIdx.loc[lastActIdx['tripID'].isna(),
                       'minBatteryLevelStart'] = endLevel  # For park acts

        lastAct = lastActIdx.reset_index('uniqueID')
        lastAct.index = lastActIn.index

        lastAct.loc[isTrip, 'minBatteryLevelStart_unlimited'] = lastAct.loc[
            isTrip, 'minBatteryLevelEnd'] + lastAct.loc[isTrip, 'drain']
        lastAct.loc[isTrip, 'minBatteryLevelStart'] = lastAct.loc[
            isTrip, 'minBatteryLevelStart_unlimited'].where(lastAct.loc[
                isTrip, 'minBatteryLevelStart_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        resNeed = lastAct.loc[isTrip, 'minBatteryLevelStart_unlimited'] - self.upperBatLev
        lastAct.loc[isTrip, 'residualNeed'] = resNeed.where(resNeed >= 0, other=0)
        return lastAct

    def __calcBatLevTripMax(self, actID: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        # Index setting of trip activities to be updated
        activeHHPersonIDs = tripActs.loc[:, 'uniqueID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]
        tripActsIdx = tripActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Index setting of previous park activities as basis for the update
        prevParkIDs = tripActs.loc[:, 'prevActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
        prevParkActsIdx = prevParkActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Calculation of battery level at start and end of trip
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelStart'] = prevParkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelEnd'].values
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelStart'] - tripActsIdx.loc[multiIdxTrip, 'drain']
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        res = tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd'] - tripActsIdx.loc[multiIdxTrip,
                                                                                    'maxBatteryLevelEnd_unlimited']
        tripActsIdx.loc[multiIdxTrip, 'maxResidualNeed'] = res.where(tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] < self.lowerBatLev, other=0)
        return tripActsIdx.reset_index()

    def __calcBatLevTripMin(self, actID: int, tripActs: pd.DataFrame, nextParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'uniqueID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]
        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(['uniqueID', 'tripID', 'parkID'])
        nextParkIDs = tripActs.loc[:, 'nextActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, nextParkIDs)]
        nextParkActsIdx = nextParkActs.set_index(['uniqueID', 'tripID', 'parkID'])
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

    # FIXME: Implement more abstract consolidated func of this and calcBatLevTripMax()
    def __calcBatLevParkMax(self, actID: int, parkActs: pd.DataFrame, prevTripActs: pd.DataFrame = None):
        """ Calculate the maximum SOC of the given parking activities for the activity ID given by actID. Previous trip
        activities are used as boundary for maxBatteryLevelStart. This function is called multiple times once per
        activity ID. It is then applied to all activities with the given activity ID in a vectorized manner.

        Args:
            actID (int): Activity ID in current loop
            parkActs (pd.DataFrame): _description_
            prevTripActs (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        # Setting next park activity battery start level to battery end level of current trip
        # Index setting of park activities to be updated
        activeHHPersonIDs = parkActs.loc[:, 'uniqueID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Index setting of previous trip activities used to update
        prevTripIDs = parkActs.loc[:, 'prevActID']
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs)]
        prevTripActsIdx = prevTripActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Calculation of battery level at start and end of park activity
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelStart'] = prevTripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd'].values
        parkActsIdx['maxBatteryLevelEnd_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelStart'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelEnd'] = parkActsIdx['maxBatteryLevelEnd_unlimited'].where(
            parkActsIdx['maxBatteryLevelEnd_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        tmpOvershoot = parkActsIdx['maxBatteryLevelEnd_unlimited'] - self.upperBatLev
        parkActsIdx['maxOvershoot'] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def __calcBatLevParkMin(self, actID: int, parkActs: pd.DataFrame, nextTripActs: pd.DataFrame = None):
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
        # Composing park activity index to be set
        activeHHPersonIDs = parkActs.loc[:, 'uniqueID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Composing trip activity index to get battery level from
        nextTripIDs = parkActs.loc[:, 'nextActID']
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, nextTripIDs)]
        nextTripActsIdx = nextTripActs.set_index(['uniqueID', 'tripID', 'parkID'])
        # Setting next park activity battery start level to battery end level of current trip
        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelEnd'] = nextTripActsIdx.loc[
            multiIdxTrip, 'minBatteryLevelStart'].values
        parkActsIdx['minBatteryLevelStart_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'minBatteryLevelEnd'] - parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'minBatteryLevelStart'] = parkActsIdx['minBatteryLevelStart_unlimited'].where(
            parkActsIdx['minBatteryLevelStart_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tmpUndershoot = parkActsIdx['minBatteryLevelStart_unlimited'] - self.lowerBatLev
        parkActsIdx['minUndershoot'] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def _uncontrolledCharging(self):
        parkActs = self.activities.loc[self.activities['tripID'].isna(),
                                       :].copy()
        parkActs['uncontrolledCharge'] = parkActs[
            'maxBatteryLevelEnd'] - parkActs['maxBatteryLevelStart']

        # Calculate timestamp at which charging ends disregarding parking end
        parkActs['timestampEndUC_unltd'] = parkActs.apply(
            lambda x: self._calcChargeEndTS(
                startTS=x['timestampStart'], startBatLev=x['maxBatteryLevelStart'],
                power=x['availablePower']), axis=1)

        # Take into account possible earlier disconnection due to end of parking
        parkActs['timestampEndUC'] = parkActs['timestampEndUC_unltd'].where(
            parkActs['timestampEndUC_unltd'] <= parkActs['timestampEnd'],
            other=parkActs['timestampEnd'])

        # This would be a neater implementation of the above, but
        # timestampEndUC_unltd contains NA making it impossible to convert to
        # datetime with .dt which is a prerequisite to applying
        # pandas.DataFrame.min()
        # parkActs['timestampEndUC'] = parkActs[
        #     ['timestampEndUC_unltd', 'timestampEnd']].min(axis=1)

        self.activities.loc[self.activities['tripID'].isna(), :] = parkActs

    def _calcChargeEndTS(self,
                         startTS: pd.Timestamp,
                         startBatLev: float,
                         power: float
                         ):
        if power == 0:
            return pd.NA
        deltaBatLev = self.upperBatLev - startBatLev
        timeForCharge = deltaBatLev / power  # in hours
        return startTS + pd.Timedelta(value=timeForCharge, unit='h').round(freq='s')

    def _auxFuelNeed(self):
        self.activities['auxiliaryFuelNeed'] = self.activities['residualNeed'] * self.flexConfig[
            'Fuel_consumption'] / self.flexConfig['Electric_consumption']

    def _filterResidualNeed(self, acts: pd.DataFrame, indexCols: list):
        """
        Filter out days (uniqueIDs) that require additional fuel, i.e. for which the trip distance cannot be
        completely be fulfilled with the available charging power. Since additional fuel for a single trip motivates
        filtering out the whole vehicle, indexCol defines the columns that make up one vehicle. If indexCols is
        ['uniqueID'], all uniqueIDs that have at least one trip requiring fuel are disregarded. If indexCols is
        ['categoryID', 'weekID'] each unique combination of categoryID and weekID (each "week") for which fuel is
        required in at least one trip is disregarded.

        Args:
            acts (pd.DataFrame): Activities data set containing at least the columns 'uniqueID' and 'maxResidualNeed'
            indexCols (list): Columns that define a "day", i.e. all unique combinations where at least one activity
                requires residual fuel are disregarded.
        """
        actsIdx = acts.set_index(indexCols)
        idxOut = (~actsIdx['maxResidualNeed'].isin([None, 0])) | (~actsIdx['minResidualNeed'].isin([None, 0]))

        if len(indexCols) == 1:
            catWeekIDOut = actsIdx.index[idxOut]
            actsFilt = actsIdx.loc[~actsIdx.index.isin(catWeekIDOut)]
        else:
            catWeekIDOut = acts.loc[idxOut.values, indexCols]
            tplFilt = catWeekIDOut.apply(lambda x: tuple(x), axis=1).unique()
            actsFilt = actsIdx.loc[~actsIdx.index.isin(tplFilt), :]
        return actsFilt.reset_index()

    def _writeOutput(self):
        if self.globalConfig["writeOutputToDisk"]["flexOutput"]:
            root = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot'])
            folder = self.globalConfig['pathRelative']['flexOutput']
            fileName = createFileName(globalConfig=self.globalConfig, manualLabel='', file='outputFlexEstimator',
                                    datasetID=self.datasetID)
            writeOut(data=self.activities, path=root / folder / fileName)

    def estimateTechnicalFlexibility_noBoundaryConstraints(self):
        """
        Main run function for the class WeekFlexEstimator. Calculates uncontrolled charging as well as technical
        boundary constraints for controlled charging and feeding electricity back into the grid on an indvidiual vehicle
        basis. If filterFuelNeed is True, only electrifiable days are considered.

        Args:
            filterFuelNeed (bool): If true, it is ensured that all trips can be fulfilled by battery electric vehicles
            specified by battery size and specific consumption as given in the config. Here, not only trips but cars
            days are filtered out.
            startBatteryLevel (float): Initial battery level of every activity chain.

        Returns:
            pd.DataFrame: Activities data set comprising uncontrolled charging and flexible charging constraints for
            each car.
        """
        self._drain()
        self._maxChargeVolumePerParkingAct()
        self.__batteryLevelMax(startLevel=self.upperBatLev * self.flexConfig['Start_SOC'])
        self._uncontrolledCharging()
        self.__batteryLevelMin()
        self._auxFuelNeed()
        if self.flexConfig['filterFuelNeed']:
            self.activities = self._filterResidualNeed(acts=self.activities, indexCols=['uniqueID'])
        self._writeOutput()
        print("Technical flexibility estimation ended.")
        return self.activities

    def estimateTechnicalFlexibilityIterating(self) -> pd.DataFrame:
        """
        Main run function for the class WeekFlexEstimator. Calculates uncontrolled charging as well as technical
        boundary constraints for controlled charging and feeding electricity back into the grid on an indvidiual vehicle
        basis. If filterFuelNeed is True, only electrifiable days are considered.

        Args:
            filterFuelNeed (bool): If true, it is ensured that all trips can be fulfilled by battery electric vehicles
            specified by battery size and specific consumption as given in the config. Here, not only trips but cars
            days are filtered out.
            startBatteryLevel (float): Initial battery level of every activity chain.

        Returns:
            pd.DataFrame: Activities data set comprising uncontrolled charging and flexible charging constraints for
            each car.
        """
        self._drain()
        self._maxChargeVolumePerParkingAct()
        self.__iterativeBatteryLevelCalculations(
            maxIter=self.flexConfig['maxIterations'], eps=self.flexConfig['epsilon_battery_level'],
            batCap=self.flexConfig['Battery_capacity'], nVehicles=len(self.activities['uniqueID'].unique())
        )
        self._auxFuelNeed()
        if self.flexConfig['filterFuelNeed']:
            self.activities = self._filterResidualNeed(acts=self.activities, indexCols=['uniqueID'])
        self._writeOutput()
        print("Technical flexibility estimation ended.")
        return self.activities

    def __iterativeBatteryLevelCalculations(self, maxIter: int, eps: float, batCap: float, nVehicles: int):
        """ A single iteration of calculation maximum battery levels, uncontrolled charging and minimum battery levels
        for each trip. Initial battery level for first iteration loop per uniqueID in index. Start battery level will be
        set to end battery level consecutively. Function operates on class attribute self.activities.

        Args:
            maxIter (int): Maximum iteration limit if epsilon threshold is never reached.
            eps (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            batCap (float): Average nominal battery capacity per vehicle in kWh.
            nVehicles (int): Number of vehicles in the empiric mobility pattern data set.
        """
        batteryLevelMaxEnd = self.upperBatLev * self.flexConfig['Start_SOC']
        batteryLevelMinStart = self.lowerBatLev
        absoluteEps = int(self.__absoluteEps(eps=eps, batCap=batCap, nVehicles=nVehicles))

        batteryLevelMaxEnd = self.__batteryLevelMax(startLevel=batteryLevelMaxEnd)
        self._uncontrolledCharging()
        batteryLevelMinStart = self.__batteryLevelMin(endLevel=batteryLevelMinStart)

        deltaMax = self.__getDelta(colStart='maxBatteryLevelStart', colEnd='maxBatteryLevelEnd')
        deltaMin = self.__getDelta(colStart='minBatteryLevelStart', colEnd='minBatteryLevelEnd')

        for i in range(1, maxIter+1):
            if deltaMax < absoluteEps and deltaMin < absoluteEps:
                break

            elif deltaMax >= absoluteEps:
                batteryLevelMaxEnd = self.__batteryLevelMax(startLevel=batteryLevelMaxEnd)
                self._uncontrolledCharging()
                deltaMax = self.__getDelta(colStart='maxBatteryLevelStart', colEnd='maxBatteryLevelEnd')

            else:
                batteryLevelMinStart = self.__batteryLevelMin(endLevel=batteryLevelMinStart)
                deltaMin = self.__getDelta(colStart='minBatteryLevelStart', colEnd='minBatteryLevelEnd')

            print(f'Finished ITERATION {i+1} / {maxIter}. Delta max battery level is {int(deltaMax)} / {absoluteEps} '
                  f'and delta min battery is {int(deltaMin)} / {absoluteEps}.')

    def __absoluteEps(self, eps: float, batCap: float, nVehicles: int) -> float:
        """Calculates the absolute threshold of battery level deviatiation used for interrupting the battery level
        calculation iterations.

        Args:
            eps (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            batteryCapacity (float): Average battery capacity per car
            nVehicles (int): Number of vehicles

        Returns:
            float: Absolute iteration threshold in kWh of fleet battery
        """
        return eps * batCap * nVehicles

    # DEPRECATED?
    def __getStartLevel(self, startLevel: float):
        lastActs = self.activities.loc[self.activities['isLastActivity'],
                                       ['uniqueID', 'maxBatteryLevelEnd']]
        lastActs['maxBatteryLevelEnd'] = startLevel
        return lastActs.set_index('uniqueID')

    # DEPRECATED?
    def __getEndLevel(self, endLevel: float):
        firstActs = self.activities.loc[self.activities['isFirstActivity'],
                                        ['uniqueID', 'minBatteryLevelStart']]
        firstActs['minBatteryLevelStart'] = endLevel
        return firstActs.set_index('uniqueID')

    def __getDelta(self, colStart: str, colEnd: str) -> float:
        return abs(self.activities.loc[self.activities[
            'isLastActivity'],
            colEnd].values - self.activities.loc[
                self.activities['isFirstActivity'],
                colStart].values).sum()


class WeekFlexEstimator(FlexEstimator):
    def __init__(self, configDict: dict, activities: pd.DataFrame, threshold: float = None):
        super().__init__(configDict=configDict, activities=activities)
        self.threshold = None
        self.thresholdSOC = None
        self.thresholdAbsolute = None
        self.__setThreshold(threshold)

    def __setThreshold(self, threshold: float):
        if threshold:
            self.useThreshold = True
            self.thresholdSOC = threshold
            self.thresholdAbsolute = threshold * self.flexConfig['Battery_capacity']
            if self.thresholdAbsolute <= self.upperBatLev:
                self.thresholdAbsolute = self.thresholdAbsolute
            else:
                self.thresholdAbsolute = self.upperBatLev
        else:
            self.useThreshold = False

    def __maxChargeVolumePerParkingActWeek(self):
        self.__calcTimedeltaONActs(ONIdx=self.activities['isSyntheticONPark'])
        self._maxChargeVolumePerParkingAct()

    def __calcTimedeltaONActs(self, ONIdx: pd.Series):
        """
        Calculate the timedelta for week activity chains between two synthetically merged days neglecting the dates (
            year-month-day) and only taking into account the start and end timestamps of the overnight parking activity.

        Args:
            ONIdx (pd.Series): Boolean series identifying overnight park activities in the week activity chain
        """
        ONActs = self.activities.loc[ONIdx, :]
        tsEndWODate = pd.to_timedelta(ONActs['timestampEnd'].dt.hour, unit='h') + pd.to_timedelta(
            ONActs['timestampEnd'].dt.minute, unit='m') + pd.to_timedelta(1, unit='d')
        tsStartWODate = pd.to_timedelta(ONActs['timestampStart'].dt.hour, unit='h') + pd.to_timedelta(
            ONActs['timestampStart'].dt.minute, unit='m')
        self.activities.loc[ONIdx, 'timedelta'] = tsEndWODate - tsStartWODate

    def _batteryLevelMax(self, startLevel: float):
        """
        Calculate the maximum battery level at the beginning and end of each activity for weekly activity chains.
        This represents the case of vehicle users always connecting when charging is available and charging as soon as
        possible as fast as possible until the maximum battery capacity is reached.
        actTemp is the overall collector for each activity's park and trip results, that will then get written to
        self.activities at the very end.

        Args:
            startLevel (float): Battery level at beginning of first activity for each activity chain
        """
        print('Starting maximum battery level calculation.')
        self.activities.loc[self.activities['isFirstActivity'], :] = self._calcMaxBatFirstAct(startLevel=startLevel)

        # Start and end for all trips and parkings in between
        setActs = range(int(self.activities['actID'].max()) + 1)
        for act in setActs:
            print(f'Calculating maximum battery level for actID (park and trip) {act}.')
            if act != 0:
                self.__shiftBatLevEnd()
                self.__maxBatLevPark(parkID=act, useThreshold=self.useThreshold)
            self.__shiftBatLevEnd()
            self.__maxBatLevTrips(tripID=act)

    def _getFirstActIdx(self):
        """
        Method to return the first activities. In the week diary, the activities are reset in strict ascending order
        currently neglecting the edge case of overnight trips. Thus all week activity chains have the exact same
        beginning of the first activity being a park activity with parkID==0, then a trip with tripID==0, then a park
        activity with parkID==1 and so forth.

        Returns:
            pd.Series: Boolean pandas Series with first activities
        """
        return self.activities['isFirstActivity']

    def __shiftBatLevEnd(self):
        """
        Shifts the battery level at end of the previous trip to current activity for battery calculation to the next
        activity. This is always called between maxBatLevTrips() and maxBatLevPark() before setting the
        variables.
        """
        self.activities['maxBatteryLevelEnd_prev'] = self.activities['maxBatteryLevelEnd'].shift(1)

    def __maxBatLevTrips(self, tripID: int):
        """
        Calculates the maximum battery level for trip activities in the week activity chain.
        """
        idx = self.activities['tripID'] == tripID

        # Calculation of battery level at start and end of trip
        self.activities.loc[idx, 'maxBatteryLevelStart'] = self.activities.loc[idx, 'maxBatteryLevelEnd_prev']
        self.activities.loc[idx, 'maxBatteryLevelEnd_unlimited'] = self.activities.loc[
            idx, 'maxBatteryLevelStart'] - self.activities.loc[idx, 'drain']
        self.activities.loc[idx, 'maxBatteryLevelEnd'] = self.activities.loc[idx, 'maxBatteryLevelEnd_unlimited'].where(
            self.activities.loc[idx, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        self.activities.loc[idx, 'maxResidualNeed'] = self.activities.loc[idx, 'maxBatteryLevelEnd_unlimited'].where(
            self.activities.loc[idx, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)

    def __maxBatLevPark(self, parkID: int, useThreshold: bool = False):
        """
        Calculates the maximum battery level for all park activities of parkID corresponding to parkID in the week
        activity chain. If useThreshold==True, charging only occurs once the batteryLevel at beginning of the activity
        falls below the threshold given in self.thresholdAbsolute.
        """
        idx = self.activities['parkID'] == parkID

        # Calculation of battery level at start park activity
        self.activities.loc[idx, 'maxBatteryLevelStart'] = self.activities.loc[idx, 'maxBatteryLevelEnd_prev']

        # Determine activities for which battery level is below threshold ("charging necessary")
        if useThreshold:
            idxT = self.activities['maxBatteryLevelStart'] < self.thresholdAbsolute
            idxCharge = idx & idxT
            idxNoCharge = (idx) & (~idxT)
        else:
            idxCharge = idx
            idxNoCharge = None

        # Intermediary batLev calculation for both cases
        self.activities.loc[idxCharge, 'maxBatteryLevelEnd_unlimited'] = self.activities.loc[
            idxCharge, 'maxBatteryLevelStart'] + self.activities.loc[idxCharge, 'maxChargeVolume']

        # Assigning new batLev end after charging or no charging
        self.activities.loc[idxCharge, 'maxBatteryLevelEnd'] = self.activities['maxBatteryLevelEnd_unlimited'].where(
            self.activities['maxBatteryLevelEnd_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        self.activities.loc[idxNoCharge, 'maxBatteryLevelEnd'] = self.activities['maxBatteryLevelStart']

        tmpOvershoot = self.activities['maxBatteryLevelEnd_unlimited'] - self.upperBatLev
        self.activities['maxOvershoot'] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)

    def _batteryLevelMin(self):
        """
        Calculate the minimum battery level at the beginning and end of each activity. This represents the case of
        vehicles just being charged for the energy required for the next trip and as late as possible. The loop works
        exactly inverted to the batteryLevelMax() function since later trips influence the energy that has to be
        charged in parking activities before. Thus, activities are looped over from the last activity to first.
        For week flexibility estimation, it can not always be assured that the last activity is a parking activity.
        """
        print('Starting minimum battery level calculation.')
        self.activities.loc[self.activities['isLastActivity'], :] = self._calcMinBatLastAct()
        self.__calcMinBatBeforeLastAct()

        # Start and end for all trips and parkings in between
        setActs = range(int(self.activities['actID'].max())-1, -1, -1)
        for act in setActs:
            print(f'Calculating minimum battery level for actID (park and trip) {act}.')
            self.__shiftBatLevStart()
            self.__minBatLevTrips(tripID=act)
            self.__shiftBatLevStart()
            self.__minBatLevPark(parkID=act)

    def __calcMinBatBeforeLastAct(self):
        """
        Calculate the minimum battery level attributes only for the activities with the same activityID as the last
        activity that are not the last activity. This function is used to start with a clean unified setup in the
        loop of __batteryLevelMin(). Those activities are all park activities.
        """
        self.__shiftBatLevStart()

        # Preliminary identification of activities
        self.activities[['isLastActivity_next', 'actID_next']] = self.activities[['isLastActivity', 'actID']].shift(-1)
        idx = (self.activities['isLastActivity_next']) & (self.activities['actID'] == self.activities['actID_next'])
        self.activities['parkActBeforeLastAct'] = idx

        # Setting of battery level attributes
        self.activities.loc[idx, 'minBatteryLevelEnd'] = self.activities.loc[idx, 'minBatteryLevelStart_next']
        self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] = self.activities.loc[
            idx, 'minBatteryLevelEnd'] - self.activities.loc[idx, 'maxChargeVolume']
        self.activities.loc[idx, 'minBatteryLevelStart'] = self.activities['minBatteryLevelStart_unlimited'].where(
            self.activities['minBatteryLevelStart_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tmpUndershoot = self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] - self.lowerBatLev
        self.activities.loc[idx, 'minUndershoot'] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)
        self.activities.drop(columns=['isLastActivity_next', 'actID_next'])

    def __shiftBatLevStart(self):
        """
        Shifts the battery level at start of the next trip to current activity for battery calculation. This is
        always called between minBatLevTrips() and minBatLevPark() before setting the battery level variables.
        """
        self.activities['minBatteryLevelStart_next'] = self.activities['minBatteryLevelStart'].shift(-1)

    def __minBatLevTrips(self, tripID: int):
        """
        Calculate minimum battery levels for given tripID. The calculated battery levels only suffice for the trips
        and thus describe a technical lower level for each activity. This function is called looping through the parking
        activities from largest to smallest. The column "minOvershoot" describes electricity volume that can be charged
        beyond the given battery capacity.

        Args:
            tripID (int): tripID for which the battery levels should be calculated
        """
        idx = (self.activities['tripID'] == tripID) & ~(self.activities['isLastActivity'])

        self.activities.loc[idx, 'minBatteryLevelEnd'] = self.activities.loc[idx, 'minBatteryLevelStart_next']
        self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] = self.activities.loc[
            idx, 'minBatteryLevelEnd'] + self.activities.loc[idx, 'drain']
        self.activities.loc[idx, 'minBatteryLevelStart'] = self.activities.loc[
            idx, 'minBatteryLevelStart_unlimited'].where(self.activities.loc[
                idx, 'minBatteryLevelStart_unlimited'] <= self.upperBatLev, other=self.upperBatLev)
        resNeed = self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] - self.upperBatLev
        self.activities.loc[idx, 'minResidualNeed'] = resNeed.where(resNeed >= 0, other=0)

    def __minBatLevPark(self, parkID: int):
        """
        Calculate minimum battery levels for given parking activities based on the given next trip activities.
        The calculated battery levels only suffice for the trips and thus describe a technical lower level for
        each activity. This function is called looping through the parking activities from largest to smallest.
        The column "minOvershoot" describes electricity volume that can be charged beyond the given battery
        capacity.

        Args:
            parkID (int): ID of the current park activities for which the battery level variables are being calculated
        """
        idx = (self.activities['parkID'] == parkID) & ~(self.activities['isLastActivity']) & ~(
            self.activities['parkActBeforeLastAct'])

        self.activities.loc[idx, 'minBatteryLevelEnd'] = self.activities.loc[idx, 'minBatteryLevelStart_next']
        self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] = self.activities.loc[
            idx, 'minBatteryLevelEnd'] - self.activities.loc[idx, 'maxChargeVolume']
        self.activities.loc[idx, 'minBatteryLevelStart'] = self.activities['minBatteryLevelStart_unlimited'].where(
            self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        tmpUndershoot = self.activities.loc[idx, 'minBatteryLevelStart_unlimited'] - self.lowerBatLev
        self.activities.loc[idx, 'minUndershoot'] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)

    def estimateWeekTechnicalFlexibility(self, filterFuelNeed: bool = True):
        """
        Main run function for the class WeekFlexEstimator. Calculates uncontrolled charging as well as technical
        boundary constraints for controlled charging and feeding electricity back into the grid on an indvidiual vehicle
        basis. If filterFuelNeed is True, only electrifiable weeks are considered.

        Args:
            filterFuelNeed (bool): If true, it is ensured that all trips can be fulfilled by battery electric vehicles
            specified by battery size and specific consumption as given in the config. Here, not only trips but cars
            weeks are filtered out if "infected" by at least one trip that cannot be electrified.

        Returns:
            pd.DataFrame: Activities data set comprising uncontrolled charging and flexible charging constraints for
            each car.
        """

        self._drain()
        self.__maxChargeVolumePerParkingActWeek()
        self._batteryLevelMax(startLevel=self.upperBatLev)
        self._uncontrolledCharging()
        self._batteryLevelMin()
        if filterFuelNeed:
            self.activities = self._filterResidualNeed(acts=self.activities, indexCols=['categoryID', 'weekID'])
        print(
            f'Technical flexibility estimation for one week ended considering the plugging threshold of {self.thresholdSOC}.')
        return self.activities
