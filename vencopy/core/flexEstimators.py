__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
from profilehooks import profile

import pandas as pd
from vencopy.utils.globalFunctions import create_file_name, write_out


class FlexEstimator:
    def __init__(self, configs: dict, activities: pd.DataFrame):
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.upperBatLev = (
            self.user_config["flexEstimators"]["battery_capacity"] * self.user_config["flexEstimators"]["maximum_soc"]
        )
        self.lowerBatLev = (
            self.user_config["flexEstimators"]["battery_capacity"] * self.user_config["flexEstimators"]["minimum_soc"]
        )
        self.activities = activities.copy()
        self.isTrip = ~self.activities["trip_id"].isna()
        self.isPark = ~self.activities["park_id"].isna()
        self.isFirstAct = self.activities["is_first_activity"].fillna(0).astype(bool)
        self.isLastAct = self.activities["is_last_activity"].fillna(0).astype(bool)

        # UC = uncontrolled charging
        self.activities[
            [
                "max_battery_level_start",
                "maxBatteryLevelEnd",
                "minBatteryLevelStart",
                "min_battery_level_end",
                "maxBatteryLevelEnd_unlimited",
                "uncontrolledCharge",
                "timestampEndUC_unltd",
                "timestamp_end_uncontrolled_charging",
                "minBatteryLevelEnd_unlimited",
                "maxResidualNeed",
                "minResidualNeed",
                "maxOvershoot",
                "minUndershoot",
                "auxiliaryFuelNeed",
            ]
        ] = None
        self.activitiesWOResidual = None

    def _drain(self):
        self.activities["drain"] = (
            self.activities["trip_distance"] * self.user_config["flexEstimators"]["Electric_consumption"] / 100
        )

    def _maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark, "maxChargeVolume"] = (
            self.activities.loc[self.isPark, "available_power"]
            * self.activities.loc[self.isPark, "time_delta"]
            / pd.time_delta("1 hour")
        )

    def __batteryLevelMax(self, startLevel: float) -> pd.Series:
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
        print("Starting maximum battery level calculation.")
        firstActs = self._calcMaxBatFirstAct(startLevel=startLevel)
        firstParkActs = firstActs.loc[~firstActs["park_id"].isna(), :]

        # The second condition is needed to circumvent duplicates with tripIDs=1
        # which are initiated above
        firstTripActs = firstActs.loc[(~firstActs["trip_id"].isna()) & (firstActs["is_first_activity"]), :]
        actTemp = pd.concat([firstParkActs, firstTripActs])

        # Start and end for all trips and parkings in between
        setActs = range(int(self.activities["park_id"].max()) + 1)
        tripActsRes = pd.DataFrame()  # Redundant?
        for act in setActs:  # implementable via groupby with actIDs as groups?
            print(f"Calculating maximum battery level for act {act}.")
            tripRows = (self.activities["trip_id"] == act) & (~self.activities["is_first_activity"])
            parkRows = (self.activities["park_id"] == act) & (~self.activities["is_first_activity"])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]

            # Filtering for the previous trips that have the current activity as
            # next activity
            prevTripActs = actTemp.loc[(actTemp["next_activity_id"] == act) & (~actTemp["trip_id"].isna()), :]

            # firstAct trips with trip_id==0 (overnight morning splits) are
            # handled in _calcMaxBatFirstAct above
            if act == 1:
                tripActsRes = self.__calcBatLevTripMax(activity_id=act, tripActs=tripActs, prevParkActs=firstParkActs)
            elif act != 0:
                # Park activities start off a new activity index e.g. parkAct 1 is always before tripAct 1
                parkActsRes = self.__calcBatLevParkMax(activity_id=act, parkActs=parkActs, prevTripActs=prevTripActs)
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
                prevParkActs = actTemp.loc[(actTemp["next_activity_id"] == act) & (~actTemp["park_id"].isna()), :]
                tripActsRes = self.__calcBatLevTripMax(activity_id=act, tripActs=tripActs, prevParkActs=prevParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            prevTripActs = tripActsRes  # Redundant?
        self.activities = actTemp.sort_values(by=["unique_id", "activity_id", "park_id"])
        return self.activities.loc[self.activities["is_last_activity"], ["unique_id", "maxBatteryLevelEnd"]].set_index(
            "unique_id"
        )

    def __batteryLevelMin(self, endLevel: pd.Series) -> pd.Series:
        """
        Calculate the minimum battery level at the beginning and end of each
        activity. This represents the case of vehicles just being charged for
        the energy required for the next trip and as late as possible. The loop
        works exactly inverted to the batteryLevelMax() function since later
        trips influence the energy that has to be charged in parking activities
        before. Thus, activities are looped over from the last activity to
        first.
        """
        print("Starting minimum battery level calculation.")
        print(f"Calculate minimum battery level for act {int(self.activities.activity_id.max())}.")
        lastActs = self._calcMinBatLastAct(endLevel=endLevel)
        actTemp = lastActs
        # Start and end for all trips and parkings starting from the last
        # activities, then looping to earlier acts
        setActs = range(int(self.activities["park_id"].max()) - 1, -1, -1)
        for act in setActs:
            print(f"Calculate minimum battery level for act {act}.")
            tripRows = (self.activities["trip_id"] == act) & (~self.activities["is_last_activity"])
            parkRows = (self.activities["park_id"] == act) & (~self.activities["is_last_activity"])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            nextParkActs = actTemp.loc[~actTemp["park_id"].isna(), :]

            tripActsRes = self.__calcBatLevTripMin(activity_id=act, tripActs=tripActs, nextParkActs=nextParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            nextTripActs = actTemp.loc[~actTemp["trip_id"].isna(), :]
            parkActsRes = self.__calcBatLevParkMin(activity_id=act, parkActs=parkActs, nextTripActs=nextTripActs)
            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
        self.activities = actTemp.sort_values(by=["unique_id", "activity_id", "park_id"], ignore_index=True)
        return self.activities.loc[self.activities["is_first_activity"], ["unique_id", "minBatteryLevelStart"]].set_index(
            "unique_id"
        )

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
        fa = firstAct.set_index("unique_id")
        fa["max_battery_level_start"] = startLevel
        fa = fa.reset_index("unique_id")
        fa.index = firstAct.index
        firstAct = fa
        isPark = ~firstAct["park_id"].isna()
        isTrip = ~firstAct["trip_id"].isna()
        firstAct.loc[isPark, "maxBatteryLevelEnd_unlimited"] = (
            firstAct["max_battery_level_start"] + firstAct["maxChargeVolume"]
        )
        firstAct.loc[isPark, "maxBatteryLevelEnd"] = firstAct.loc[isPark, "maxBatteryLevelEnd_unlimited"].where(
            firstAct.loc[isPark, "maxBatteryLevelEnd_unlimited"] <= self.upperBatLev, other=self.upperBatLev
        )
        firstAct.loc[isPark, "maxOvershoot"] = firstAct["maxBatteryLevelEnd_unlimited"] - firstAct["maxBatteryLevelEnd"]
        firstAct.loc[isTrip, "maxBatteryLevelEnd_unlimited"] = (
            firstAct.loc[isTrip, "max_battery_level_start"] - firstAct.loc[isTrip, "drain"]
        )
        firstAct.loc[isTrip, "maxBatteryLevelEnd"] = firstAct.loc[isTrip, "maxBatteryLevelEnd_unlimited"].where(
            firstAct.loc[isTrip, "maxBatteryLevelEnd_unlimited"] >= self.lowerBatLev, other=self.lowerBatLev
        )
        res = firstAct.loc[isTrip, "maxBatteryLevelEnd"] - firstAct.loc[isTrip, "maxBatteryLevelEnd_unlimited"]
        firstAct.loc[isTrip, "maxResidualNeed"] = res.where(
            firstAct.loc[isTrip, "maxBatteryLevelEnd_unlimited"] < self.lowerBatLev, other=0
        )
        return firstAct

    def __getFirstActIdx(self) -> pd.Series:
        """Get indices of all activities that should be treated here. These comprise not only the first activities
        determined by the column is_first_activity but also the split-up overnight trips with the trip_id==0 and the first
        parking activities with park_id==1. This method is overwritten in the FlexEstimatorWeek.

        Returns:
            pd.Series: Boolean Series identifying the relevant rows for calculating SOCs for first activities
        """
        return (self.activities["is_first_activity"]) | (self.activities["park_id"] == 1)

    def _calcMinBatLastAct(self, endLevel: pd.Series) -> pd.DataFrame:
        """Calculate the minimum battery levels for the last activity in the data set determined by the maximum activity
        ID.

        Args:
            endLevel (float or pd.Series): End battery level at end of simulation time (last_bin). Defaults to
            self.lowerBatLev, the minimum battery level. Can be either of type float (in first iteration) or pd.Series
            with respective unique_id in the index.
        Returns:
            pd.DataFrame: Activity data set with the battery variables set for all last activities of the activity
            chains
        """
        # Last activities - parking and trips
        lastActIn = self.activities.loc[self.activities["is_last_activity"], :].copy()
        isTrip = ~lastActIn["trip_id"].isna()

        lastActIdx = lastActIn.set_index("unique_id")
        lastActIdx["min_battery_level_end"] = endLevel
        lastActIdx.loc[lastActIdx["trip_id"].isna(), "minBatteryLevelStart"] = endLevel  # For park acts

        lastAct = lastActIdx.reset_index("unique_id")
        lastAct.index = lastActIn.index

        lastAct.loc[isTrip, "minBatteryLevelStart_unlimited"] = (
            lastAct.loc[isTrip, "min_battery_level_end"] + lastAct.loc[isTrip, "drain"]
        )
        lastAct.loc[isTrip, "minBatteryLevelStart"] = lastAct.loc[isTrip, "minBatteryLevelStart_unlimited"].where(
            lastAct.loc[isTrip, "minBatteryLevelStart_unlimited"] <= self.upperBatLev, other=self.upperBatLev
        )
        resNeed = lastAct.loc[isTrip, "minBatteryLevelStart_unlimited"] - self.upperBatLev
        lastAct.loc[isTrip, "residual_need"] = resNeed.where(resNeed >= 0, other=0)
        return lastAct

    def __calcBatLevTripMax(
        self, activity_id: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame = None
    ) -> pd.DataFrame:
        # Setting trip activity battery start level to battery end level of previous parking
        # Index setting of trip activities to be updated
        activeHHPersonIDs = tripActs.loc[:, "unique_id"]
        multiIdxTrip = [(id, activity_id, None) for id in activeHHPersonIDs]
        tripActsIdx = tripActs.set_index(["unique_id", "trip_id", "park_id"])
        # Index setting of previous park activities as basis for the update
        prevParkIDs = tripActs.loc[:, "prevActID"]
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
        prevParkActsIdx = prevParkActs.set_index(["unique_id", "trip_id", "park_id"])
        # Calculation of battery level at start and end of trip
        tripActsIdx.loc[multiIdxTrip, "max_battery_level_start"] = prevParkActsIdx.loc[
            multiIdxPark, "maxBatteryLevelEnd"
        ].values
        tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd_unlimited"] = (
            tripActsIdx.loc[multiIdxTrip, "max_battery_level_start"] - tripActsIdx.loc[multiIdxTrip, "drain"]
        )
        tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd"] = tripActsIdx.loc[
            multiIdxTrip, "maxBatteryLevelEnd_unlimited"
        ].where(
            tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd_unlimited"] >= self.lowerBatLev, other=self.lowerBatLev
        )
        res = (
            tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd"]
            - tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd_unlimited"]
        )
        tripActsIdx.loc[multiIdxTrip, "maxResidualNeed"] = res.where(
            tripActsIdx.loc[multiIdxTrip, "maxBatteryLevelEnd_unlimited"] < self.lowerBatLev, other=0
        )
        return tripActsIdx.reset_index()

    def __calcBatLevTripMin(
        self, activity_id: int, tripActs: pd.DataFrame, nextParkActs: pd.DataFrame = None
    ) -> pd.DataFrame:
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, "unique_id"]
        multiIdxTrip = [(id, activity_id, None) for id in activeHHPersonIDs]
        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(["unique_id", "trip_id", "park_id"])
        nextParkIDs = tripActs.loc[:, "next_activity_id"]
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, nextParkIDs)]
        nextParkActsIdx = nextParkActs.set_index(["unique_id", "trip_id", "park_id"])
        tripActsIdx.loc[multiIdxTrip, "min_battery_level_end"] = nextParkActsIdx.loc[
            multiIdxPark, "minBatteryLevelStart"
        ].values

        # Setting minimum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, "minBatteryLevelStart_unlimited"] = (
            tripActsIdx.loc[multiIdxTrip, "min_battery_level_end"] + tripActsIdx.loc[multiIdxTrip, "drain"]
        )
        tripActsIdx.loc[multiIdxTrip, "minBatteryLevelStart"] = tripActsIdx.loc[
            multiIdxTrip, "minBatteryLevelStart_unlimited"
        ].where(
            tripActsIdx.loc[multiIdxTrip, "minBatteryLevelStart_unlimited"] <= self.upperBatLev, other=self.upperBatLev
        )
        resNeed = tripActsIdx.loc[multiIdxTrip, "minBatteryLevelStart_unlimited"] - self.upperBatLev
        tripActsIdx.loc[multiIdxTrip, "minResidualNeed"] = resNeed.where(resNeed >= 0, other=0)
        return tripActsIdx.reset_index()

    def __calcBatLevParkMax(
        self, activity_id: int, parkActs: pd.DataFrame, prevTripActs: pd.DataFrame = None
    ) -> pd.DataFrame:
        """Calculate the maximum SOC of the given parking activities for the activity ID given by activity_id. Previous trip
        activities are used as boundary for max_battery_level_start. This function is called multiple times once per
        activity ID. It is then applied to all activities with the given activity ID in a vectorized manner.

        Args:
            activity_id (int): Activity ID in current loop
            parkActs (pd.DataFrame): _description_
            prevTripActs (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Park activities with maximum battery level columns.
        """
        # Setting next park activity battery start level to battery end level of current trip
        # Index setting of park activities to be updated
        activeHHPersonIDs = parkActs.loc[:, "unique_id"]
        multiIdxPark = [(id, None, activity_id) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(["unique_id", "trip_id", "park_id"])

        # Index setting of previous trip activities used to update
        prevTripIDs = parkActs.loc[:, "prevActID"]
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs)]
        prevTripActsIdx = prevTripActs.set_index(["unique_id", "trip_id", "park_id"])

        # Calculation of battery level at start and end of park activity
        parkActsIdx.loc[multiIdxPark, "max_battery_level_start"] = prevTripActsIdx.loc[
            multiIdxTrip, "maxBatteryLevelEnd"
        ].values
        parkActsIdx["maxBatteryLevelEnd_unlimited"] = (
            parkActsIdx.loc[multiIdxPark, "max_battery_level_start"] + parkActsIdx.loc[multiIdxPark, "maxChargeVolume"]
        )
        parkActsIdx.loc[multiIdxPark, "maxBatteryLevelEnd"] = parkActsIdx["maxBatteryLevelEnd_unlimited"].where(
            parkActsIdx["maxBatteryLevelEnd_unlimited"] <= self.upperBatLev, other=self.upperBatLev
        )
        tmpOvershoot = parkActsIdx["maxBatteryLevelEnd_unlimited"] - self.upperBatLev
        parkActsIdx["maxOvershoot"] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def __calcBatLevParkMin(
        self, activity_id: int, parkActs: pd.DataFrame, nextTripActs: pd.DataFrame = None
    ) -> pd.DataFrame:
        """Calculate minimum battery levels for given parking activities based on the given next trip activities.
        The calculated battery levels only suffice for the trips and thus describe a technical lower level for
        each activity. This function is called looping through the parking activities from largest to smallest.
        The column "minOvershoot" describes electricity volume that can be charged beyond the given battery
        capacity.

        Args:
            activity_id (int): _description_
            parkActs (pd.DataFrame): _description_
            nextTripActs (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        # Composing park activity index to be set
        activeHHPersonIDs = parkActs.loc[:, "unique_id"]
        multiIdxPark = [(id, None, activity_id) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(["unique_id", "trip_id", "park_id"])
        # Composing trip activity index to get battery level from
        nextTripIDs = parkActs.loc[:, "next_activity_id"]
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, nextTripIDs)]
        nextTripActsIdx = nextTripActs.set_index(["unique_id", "trip_id", "park_id"])
        # Setting next park activity battery start level to battery end level of current trip
        parkActsIdx.loc[multiIdxPark, "min_battery_level_end"] = nextTripActsIdx.loc[
            multiIdxTrip, "minBatteryLevelStart"
        ].values
        parkActsIdx["minBatteryLevelStart_unlimited"] = (
            parkActsIdx.loc[multiIdxPark, "min_battery_level_end"] - parkActsIdx.loc[multiIdxPark, "maxChargeVolume"]
        )
        parkActsIdx.loc[multiIdxPark, "minBatteryLevelStart"] = parkActsIdx["minBatteryLevelStart_unlimited"].where(
            parkActsIdx["minBatteryLevelStart_unlimited"] >= self.lowerBatLev, other=self.lowerBatLev
        )
        tmpUndershoot = parkActsIdx["minBatteryLevelStart_unlimited"] - self.lowerBatLev
        parkActsIdx["minUndershoot"] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def _uncontrolledCharging(self):
        parkActs = self.activities.loc[self.activities["trip_id"].isna(), :].copy()
        parkActs["uncontrolledCharge"] = parkActs["maxBatteryLevelEnd"] - parkActs["max_battery_level_start"]

        # Calculate timestamp at which charging ends disregarding parking end
        parkActs["timestampEndUC_unltd"] = parkActs.apply(
            lambda x: self._calcChargeEndTS(
                startTS=x["timestamp_start"], startBatLev=x["max_battery_level_start"], power=x["available_power"]
            ),
            axis=1,
        )

        # Take into account possible earlier disconnection due to end of parking
        parkActs["timestamp_end_uncontrolled_charging"] = parkActs["timestampEndUC_unltd"].where(
            parkActs["timestampEndUC_unltd"] <= parkActs["timestamp_end"], other=parkActs["timestamp_end"]
        )

        # This would be a neater implementation of the above, but
        # timestampEndUC_unltd contains NA making it impossible to convert to
        # datetime with .dt which is a prerequisite to applying
        # pandas.DataFrame.min()
        # parkActs['timestamp_end_uncontrolled_charging'] = parkActs[
        #     ['timestampEndUC_unltd', 'timestamp_end']].min(axis=1)

        self.activities.loc[self.activities["trip_id"].isna(), :] = parkActs

    def _calcChargeEndTS(self, startTS: pd.Timestamp, startBatLev: float, power: float) -> pd.Timestamp:
        if power == 0:
            return pd.NA
        deltaBatLev = self.upperBatLev - startBatLev
        timeForCharge = deltaBatLev / power  # in hours
        return startTS + pd.time_delta(value=timeForCharge, unit="h").round(freq="s")

    def _auxFuelNeed(self):
        self.activities["auxiliaryFuelNeed"] = (
            self.activities["residual_need"]
            * self.user_config["flexEstimators"]["Fuel_consumption"]
            / self.user_config["flexEstimators"]["Electric_consumption"]
        )

    def _filterResidualNeed(self, acts: pd.DataFrame, indexCols: list) -> pd.DataFrame:
        """
        Filter out days (uniqueIDs) that require additional fuel, i.e. for which the trip distance cannot be
        completely be fulfilled with the available charging power. Since additional fuel for a single trip motivates
        filtering out the whole vehicle, indexCol defines the columns that make up one vehicle. If indexCols is
        ['unique_id'], all uniqueIDs that have at least one trip requiring fuel are disregarded. If indexCols is
        ['categoryID', 'weekID'] each unique combination of categoryID and weekID (each "week") for which fuel is
        required in at least one trip is disregarded.

        Args:
            acts (pd.DataFrame): Activities data set containing at least the columns 'unique_id' and 'maxResidualNeed'
            indexCols (list): Columns that define a "day", i.e. all unique combinations where at least one activity
                requires residual fuel are disregarded.
        """
        actsIdx = acts.set_index(indexCols)
        idxOut = (~actsIdx["maxResidualNeed"].isin([None, 0])) | (~actsIdx["minResidualNeed"].isin([None, 0]))

        if len(indexCols) == 1:
            catWeekIDOut = actsIdx.index[idxOut]
            actsFilt = actsIdx.loc[~actsIdx.index.isin(catWeekIDOut)]
        else:
            catWeekIDOut = acts.loc[idxOut.values, indexCols]
            tplFilt = catWeekIDOut.apply(lambda x: tuple(x), axis=1).unique()
            actsFilt = actsIdx.loc[~actsIdx.index.isin(tplFilt), :]
        return actsFilt.reset_index()

    def __write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["flexOutput"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["flexOutput"]
            file_name = create_file_name(
                user_config=self.user_config,
                dev_config=self.dev_config,
                manual_label="",
                file_name_id="outputFlexEstimator",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)

    def estimateTechnicalFlexibility_noBoundaryConstraints(self) -> pd.DataFrame:
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
        self.__batteryLevelMax(startLevel=self.upperBatLev * self.user_config["flexEstimators"]["Start_SOC"])
        self._uncontrolledCharging()
        self.__batteryLevelMin()
        self._auxFuelNeed()
        if self.user_config["flexEstimators"]["filterFuelNeed"]:
            self.activities = self._filterResidualNeed(acts=self.activities, indexCols=["unique_id"])
        if self.user_config["global"]["write_output_to_disk"]["flexOutput"]:
            self.__write_output()
        print("Technical flexibility estimation ended.")
        return self.activities

    def estimate_technical_flexibility_through_iteration(self) -> pd.DataFrame:
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
            maxIter=self.user_config["flexEstimators"]["maxIterations"],
            eps=self.user_config["flexEstimators"]["epsilon_battery_level"],
            batCap=self.user_config["flexEstimators"]["battery_capacity"],
            nVehicles=len(self.activities["unique_id"].unique()),
        )
        self._auxFuelNeed()
        if self.user_config["flexEstimators"]["filterFuelNeed"]:
            self.activities = self._filterResidualNeed(acts=self.activities, indexCols=["unique_id"])
        if self.user_config["global"]["write_output_to_disk"]["flexOutput"]:
            self.__write_output()
        print("Technical flexibility estimation ended.")
        return self.activities

    def __iterativeBatteryLevelCalculations(self, maxIter: int, eps: float, batCap: float, nVehicles: int):
        """A single iteration of calculation maximum battery levels, uncontrolled charging and minimum battery levels
        for each trip. Initial battery level for first iteration loop per unique_id in index. Start battery level will be
        set to end battery level consecutively. Function operates on class attribute self.activities.

        Args:
            maxIter (int): Maximum iteration limit if epsilon threshold is never reached.
            eps (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            batCap (float): Average nominal battery capacity per vehicle in kWh.
            nVehicles (int): Number of vehicles in the empiric mobility pattern data set.
        """
        batteryLevelMaxEnd = self.upperBatLev * self.user_config["flexEstimators"]["Start_SOC"]
        batteryLevelMinStart = self.lowerBatLev
        absoluteEps = int(self.__absoluteEps(eps=eps, batCap=batCap, nVehicles=nVehicles))

        batteryLevelMaxEnd = self.__batteryLevelMax(startLevel=batteryLevelMaxEnd)
        self._uncontrolledCharging()
        batteryLevelMinStart = self.__batteryLevelMin(endLevel=batteryLevelMinStart)

        deltaMax = self.__getDelta(colStart="max_battery_level_start", colEnd="maxBatteryLevelEnd")
        deltaMin = self.__getDelta(colStart="minBatteryLevelStart", colEnd="min_battery_level_end")

        print(
            f"Finished ITERATION {1} / {maxIter}. Delta max battery level is {int(deltaMax)} / {absoluteEps} "
            f"and delta min battery is {int(deltaMin)} / {absoluteEps}."
        )

        for i in range(1, maxIter + 1):
            if deltaMax < absoluteEps and deltaMin < absoluteEps:
                break

            elif deltaMax >= absoluteEps:
                batteryLevelMaxEnd = self.__batteryLevelMax(startLevel=batteryLevelMaxEnd)
                self._uncontrolledCharging()
                deltaMax = self.__getDelta(colStart="max_battery_level_start", colEnd="maxBatteryLevelEnd")

            else:
                batteryLevelMinStart = self.__batteryLevelMin(endLevel=batteryLevelMinStart)
                deltaMin = self.__getDelta(colStart="minBatteryLevelStart", colEnd="min_battery_level_end")

            print(
                f"Finished ITERATION {i} / {maxIter}. Delta max battery level is {int(deltaMax)} / {absoluteEps} "
                f"and delta min battery is {int(deltaMin)} / {absoluteEps}."
            )

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

    def __getDelta(self, colStart: str, colEnd: str) -> float:
        return abs(
            self.activities.loc[self.activities["is_last_activity"], colEnd].values
            - self.activities.loc[self.activities["is_first_activity"], colStart].values
        ).sum()
