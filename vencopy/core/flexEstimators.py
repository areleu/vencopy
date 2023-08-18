__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path

import pandas as pd
from vencopy.utils.globalFunctions import create_file_name, write_out


class FlexEstimator:
    def __init__(self, configs: dict, activities: pd.DataFrame):
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.upper_battery_level = (
            self.user_config["flexEstimators"]["battery_capacity"] * self.user_config["flexEstimators"]["maximum_soc"]
        )
        self.lower_battery_level = (
            self.user_config["flexEstimators"]["battery_capacity"] * self.user_config["flexEstimators"]["minimum_soc"]
        )
        self.activities = activities.copy()
        self.is_trip = ~self.activities["trip_id"].isna()
        self.is_park = ~self.activities["park_id"].isna()
        self.is_first_activity = self.activities["is_first_activity"].fillna(0).astype(bool)
        self.is_last_activity = self.activities["is_last_activity"].fillna(0).astype(bool)

        # UC = uncontrolled charging
        self.activities[
            [
                "max_battery_level_start",
                "max_battery_level_end",
                "min_battery_level_start",
                "min_battery_level_end",
                "max_battery_level_end_unlimited",
                "uncontrolled_charging",
                "timestamp_end_uncontrolled_charging_unlimited",
                "timestamp_end_uncontrolled_charging",
                "min_battery_level_end_unlimited",
                "max_residual_need",
                "min_residual_need",
                "max_overshoot",
                "min_undershoot",
                "auxiliary_fuel_need",
            ]
        ] = None
        self.activities_without_residual = None

    def _drain(self):
        self.activities["drain"] = (
            self.activities["trip_distance"] * self.user_config["flexEstimators"]["electric_consumption"] / 100
        )

    def _max_charge_volume_per_parking_activity(self):
        self.activities.loc[self.is_park, "max_charge_volume"] = (
            self.activities.loc[self.is_park, "available_power"]
            * self.activities.loc[self.is_park, "time_delta"]
            / pd.Timedelta("1 hour")
        )

    def __battery_level_max(self, start_level: float) -> pd.Series:
        """
        Calculate the maximum battery level at the beginning and end of each
        activity. This represents the case of vehicle users always connecting
        when charging is available and charging as soon as possible as fast as
        possible until the maximum battery capacity is reached. actTemp is the
        overall collector for each activity's park and trip results, that will
        then get written to self.activities at the very end.

        Args:
            start_level (float): Battery start level for first activity of the
            activity chain
        """
        print("Starting maximum battery level calculation.")
        first_activities = self._calculate_max_battery_level_first_activity(start_level=start_level)
        first_parking_activities = first_activities.loc[~first_activities["park_id"].isna(), :]

        # The second condition is needed to circumvent duplicates with tripIDs=1
        # which are initiated above
        first_trip_activities = first_activities.loc[(~first_activities["trip_id"].isna()) & (first_activities["is_first_activity"]), :]
        actTemp = pd.concat([first_parking_activities, first_trip_activities])

        # Start and end for all trips and parkings in between
        setActs = range(int(self.activities["park_id"].max()) + 1)
        tripActsRes = pd.DataFrame()  # Redundant?
        for act in setActs:  # implementable via groupby with actIDs as groups?
            print(f"Calculating maximum battery level for act {act}.")
            tripRows = (self.activities["trip_id"] == act) & (~self.activities["is_first_activity"])
            parkRows = (self.activities["park_id"] == act) & (~self.activities["is_first_activity"])
            tripActs = self.activities.loc[tripRows, :]
            park_activities = self.activities.loc[parkRows, :]

            # Filtering for the previous trips that have the current activity as
            # next activity
            previous_trip_activities = actTemp.loc[(actTemp["next_activity_id"] == act) & (~actTemp["trip_id"].isna()), :]

            # firstAct trips with trip_id==0 (overnight morning splits) are
            # handled in _calculate_max_battery_level_first_activity above
            if act == 1:
                tripActsRes = self.__calculate_max_battery_level_trip(activity_id=act, tripActs=tripActs, prevParkActs=first_parking_activities)
            elif act != 0:
                # Park activities start off a new activity index e.g. parkAct 1 is always before tripAct 1
                parkActsRes = self.__calculate_max_battery_level_park(activity_id=act, park_activities=park_activities, previous_trip_activities=previous_trip_activities)
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
                prevParkActs = actTemp.loc[(actTemp["next_activity_id"] == act) & (~actTemp["park_id"].isna()), :]
                tripActsRes = self.__calculate_max_battery_level_trip(activity_id=act, tripActs=tripActs, prevParkActs=prevParkActs)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            previous_trip_activities = tripActsRes  # Redundant?
        self.activities = actTemp.sort_values(by=["unique_id", "activity_id", "park_id"])
        return self.activities.loc[self.activities["is_last_activity"], ["unique_id", "max_battery_level_end"]].set_index(
            "unique_id"
        )

    def __battery_level_min(self, endLevel: pd.Series) -> pd.Series:
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
        lastActs = self._calculate_min_battery_level_last_activity(endLevel=endLevel)
        actTemp = lastActs
        # Start and end for all trips and parkings starting from the last
        # activities, then looping to earlier activities
        setActs = range(int(self.activities["park_id"].max()) - 1, -1, -1)
        for act in setActs:
            print(f"Calculate minimum battery level for act {act}.")
            tripRows = (self.activities["trip_id"] == act) & (~self.activities["is_last_activity"])
            parkRows = (self.activities["park_id"] == act) & (~self.activities["is_last_activity"])
            tripActs = self.activities.loc[tripRows, :]
            park_activities = self.activities.loc[parkRows, :]
            next_park_activities = actTemp.loc[~actTemp["park_id"].isna(), :]

            tripActsRes = self.__calculate_min_battery_level_trip(activity_id=act, tripActs=tripActs, next_park_activities=next_park_activities)
            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            next_trip_activities = actTemp.loc[~actTemp["trip_id"].isna(), :]
            parkActsRes = self.__calculate_min_battery_level_park(activity_id=act, park_activities=park_activities, next_trip_activities=next_trip_activities)
            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
        self.activities = actTemp.sort_values(by=["unique_id", "activity_id", "park_id"], ignore_index=True)
        return self.activities.loc[self.activities["is_first_activity"], ["unique_id", "min_battery_level_start"]].set_index(
            "unique_id"
        )

    def _calculate_max_battery_level_first_activity(self, start_level: float) -> pd.DataFrame:
        """
        Calculate maximum battery levels at beginning and end of the first activities. If overnight trips are split
        up, not only first activities are being treated (see details in docstring of self._getFirstActIdx())

        Args:
            start_level (float): Start battery level at beginning of simulation (MON, 00:00). Defaults to
            self.upper_battery_level, the maximum battery level.
        Returns:
            pd.DataFrame: First activities with all battery level columns as anchor for the consecutive calculation
            of maximum charge
        """
        # First activities - parking and trips
        idx = self.__get_indeces_first_activity()
        firstAct = self.activities.loc[idx, :].copy()
        fa = firstAct.set_index("unique_id")
        fa["max_battery_level_start"] = start_level
        fa = fa.reset_index("unique_id")
        fa.index = firstAct.index
        firstAct = fa
        isPark = ~firstAct["park_id"].isna()
        isTrip = ~firstAct["trip_id"].isna()
        firstAct.loc[isPark, "max_battery_level_end_unlimited"] = (
            firstAct["max_battery_level_start"] + firstAct["max_charge_volume"]
        )
        firstAct.loc[isPark, "max_battery_level_end"] = firstAct.loc[isPark, "max_battery_level_end_unlimited"].where(
            firstAct.loc[isPark, "max_battery_level_end_unlimited"] <= self.upper_battery_level, other=self.upper_battery_level
        )
        firstAct.loc[isPark, "max_overshoot"] = firstAct["max_battery_level_end_unlimited"] - firstAct["max_battery_level_end"]
        firstAct.loc[isTrip, "max_battery_level_end_unlimited"] = (
            firstAct.loc[isTrip, "max_battery_level_start"] - firstAct.loc[isTrip, "drain"]
        )
        firstAct.loc[isTrip, "max_battery_level_end"] = firstAct.loc[isTrip, "max_battery_level_end_unlimited"].where(
            firstAct.loc[isTrip, "max_battery_level_end_unlimited"] >= self.lower_battery_level, other=self.lower_battery_level
        )
        res = firstAct.loc[isTrip, "max_battery_level_end"] - firstAct.loc[isTrip, "max_battery_level_end_unlimited"]
        firstAct.loc[isTrip, "max_residual_need"] = res.where(
            firstAct.loc[isTrip, "max_battery_level_end_unlimited"] < self.lower_battery_level, other=0
        )
        return firstAct

    def __get_indeces_first_activity(self) -> pd.Series:
        """
        Get indices of all activities that should be treated here. These comprise not only the first activities
        determined by the column is_first_activity but also the split-up overnight trips with the trip_id==0 and the first
        parking activities with park_id==1. This method is overwritten in the FlexEstimatorWeek.

        Returns:
            pd.Series: Boolean Series identifying the relevant rows for calculating SOCs for first activities
        """
        return (self.activities["is_first_activity"]) | (self.activities["park_id"] == 1)

    def _calculate_min_battery_level_last_activity(self, endLevel: pd.Series) -> pd.DataFrame:
        """Calculate the minimum battery levels for the last activity in the data set determined by the maximum activity
        ID.

        Args:
            endLevel (float or pd.Series): End battery level at end of simulation time (last_bin). Defaults to
            self.lower_battery_level, the minimum battery level. Can be either of type float (in first iteration) or pd.Series
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
        lastActIdx.loc[lastActIdx["trip_id"].isna(), "min_battery_level_start"] = endLevel  # For park activities

        lastAct = lastActIdx.reset_index("unique_id")
        lastAct.index = lastActIn.index

        lastAct.loc[isTrip, "min_battery_level_start_unlimited"] = (
            lastAct.loc[isTrip, "min_battery_level_end"] + lastAct.loc[isTrip, "drain"]
        )
        lastAct.loc[isTrip, "min_battery_level_start"] = lastAct.loc[isTrip, "min_battery_level_start_unlimited"].where(
            lastAct.loc[isTrip, "min_battery_level_start_unlimited"] <= self.upper_battery_level, other=self.upper_battery_level
        )
        resNeed = lastAct.loc[isTrip, "min_battery_level_start_unlimited"] - self.upper_battery_level
        lastAct.loc[isTrip, "residual_need"] = resNeed.where(resNeed >= 0, other=0)
        return lastAct

    def __calculate_max_battery_level_trip(
        self, activity_id: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame = None
    ) -> pd.DataFrame:
        # Setting trip activity battery start level to battery end level of previous parking
        # Index setting of trip activities to be updated
        active_unique_ids = tripActs.loc[:, "unique_id"]
        multiIdxTrip = [(id, activity_id, None) for id in active_unique_ids]
        tripActsIdx = tripActs.set_index(["unique_id", "trip_id", "park_id"])
        # Index setting of previous park activities as basis for the update
        previous_park_ids = tripActs.loc[:, "prevActID"]
        multiIdxPark = [(id, None, act) for id, act in zip(active_unique_ids, previous_park_ids)]
        prevParkActsIdx = prevParkActs.set_index(["unique_id", "trip_id", "park_id"])
        # Calculation of battery level at start and end of trip
        tripActsIdx.loc[multiIdxTrip, "max_battery_level_start"] = prevParkActsIdx.loc[
            multiIdxPark, "max_battery_level_end"
        ].values
        tripActsIdx.loc[multiIdxTrip, "max_battery_level_end_unlimited"] = (
            tripActsIdx.loc[multiIdxTrip, "max_battery_level_start"] - tripActsIdx.loc[multiIdxTrip, "drain"]
        )
        tripActsIdx.loc[multiIdxTrip, "max_battery_level_end"] = tripActsIdx.loc[
            multiIdxTrip, "max_battery_level_end_unlimited"
        ].where(
            tripActsIdx.loc[multiIdxTrip, "max_battery_level_end_unlimited"] >= self.lower_battery_level, other=self.lower_battery_level
        )
        res = (
            tripActsIdx.loc[multiIdxTrip, "max_battery_level_end"]
            - tripActsIdx.loc[multiIdxTrip, "max_battery_level_end_unlimited"]
        )
        tripActsIdx.loc[multiIdxTrip, "max_residual_need"] = res.where(
            tripActsIdx.loc[multiIdxTrip, "max_battery_level_end_unlimited"] < self.lower_battery_level, other=0
        )
        return tripActsIdx.reset_index()

    def __calculate_min_battery_level_trip(
        self, activity_id: int, tripActs: pd.DataFrame, next_park_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        # Setting trip activity battery start level to battery end level of previous parking
        active_unique_ids = tripActs.loc[:, "unique_id"]
        multiIdxTrip = [(id, activity_id, None) for id in active_unique_ids]
        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(["unique_id", "trip_id", "park_id"])
        next_park_ids = tripActs.loc[:, "next_activity_id"]
        multiIdxPark = [(id, None, act) for id, act in zip(active_unique_ids, next_park_ids)]
        nextParkActsIdx = next_park_activities.set_index(["unique_id", "trip_id", "park_id"])
        tripActsIdx.loc[multiIdxTrip, "min_battery_level_end"] = nextParkActsIdx.loc[
            multiIdxPark, "min_battery_level_start"
        ].values

        # Setting minimum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, "min_battery_level_start_unlimited"] = (
            tripActsIdx.loc[multiIdxTrip, "min_battery_level_end"] + tripActsIdx.loc[multiIdxTrip, "drain"]
        )
        tripActsIdx.loc[multiIdxTrip, "min_battery_level_start"] = tripActsIdx.loc[
            multiIdxTrip, "min_battery_level_start_unlimited"
        ].where(
            tripActsIdx.loc[multiIdxTrip, "min_battery_level_start_unlimited"] <= self.upper_battery_level, other=self.upper_battery_level
        )
        resNeed = tripActsIdx.loc[multiIdxTrip, "min_battery_level_start_unlimited"] - self.upper_battery_level
        tripActsIdx.loc[multiIdxTrip, "min_residual_need"] = resNeed.where(resNeed >= 0, other=0)
        return tripActsIdx.reset_index()

    def __calculate_max_battery_level_park(
        self, activity_id: int, park_activities: pd.DataFrame, previous_trip_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Calculate the maximum SOC of the given parking activities for the activity ID given by activity_id. Previous trip
        activities are used as boundary for max_battery_level_start. This function is called multiple times once per
        activity ID. It is then applied to all activities with the given activity ID in a vectorized manner.

        Args:
            activity_id (int): Activity ID in current loop
            park_activities (pd.DataFrame): _description_
            previous_trip_activities (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Park activities with maximum battery level columns.
        """
        # Setting next park activity battery start level to battery end level of current trip
        # Index setting of park activities to be updated
        active_unique_ids = park_activities.loc[:, "unique_id"]
        multiIdxPark = [(id, None, activity_id) for id in active_unique_ids]
        parkActsIdx = park_activities.set_index(["unique_id", "trip_id", "park_id"])

        # Index setting of previous trip activities used to update
        prevTripIDs = park_activities.loc[:, "prevActID"]
        multiIdxTrip = [(id, act, None) for id, act in zip(active_unique_ids, prevTripIDs)]
        prevTripActsIdx = previous_trip_activities.set_index(["unique_id", "trip_id", "park_id"])

        # Calculation of battery level at start and end of park activity
        parkActsIdx.loc[multiIdxPark, "max_battery_level_start"] = prevTripActsIdx.loc[
            multiIdxTrip, "max_battery_level_end"
        ].values
        parkActsIdx["max_battery_level_end_unlimited"] = (
            parkActsIdx.loc[multiIdxPark, "max_battery_level_start"] + parkActsIdx.loc[multiIdxPark, "max_charge_volume"]
        )
        parkActsIdx.loc[multiIdxPark, "max_battery_level_end"] = parkActsIdx["max_battery_level_end_unlimited"].where(
            parkActsIdx["max_battery_level_end_unlimited"] <= self.upper_battery_level, other=self.upper_battery_level
        )
        tmpOvershoot = parkActsIdx["max_battery_level_end_unlimited"] - self.upper_battery_level
        parkActsIdx["max_overshoot"] = tmpOvershoot.where(tmpOvershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def __calculate_min_battery_level_park(
        self, activity_id: int, park_activities: pd.DataFrame, next_trip_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Calculate minimum battery levels for given parking activities based on the given next trip activities.
        The calculated battery levels only suffice for the trips and thus describe a technical lower level for
        each activity. This function is called looping through the parking activities from largest to smallest.
        The column "minOvershoot" describes electricity volume that can be charged beyond the given battery
        capacity.

        Args:
            activity_id (int): _description_
            park_activities (pd.DataFrame): _description_
            next_trip_activities (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        # Composing park activity index to be set
        active_unique_ids = park_activities.loc[:, "unique_id"]
        multiIdxPark = [(id, None, activity_id) for id in active_unique_ids]
        parkActsIdx = park_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Composing trip activity index to get battery level from
        nextTripIDs = park_activities.loc[:, "next_activity_id"]
        multiIdxTrip = [(id, act, None) for id, act in zip(active_unique_ids, nextTripIDs)]
        nextTripActsIdx = next_trip_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Setting next park activity battery start level to battery end level of current trip
        parkActsIdx.loc[multiIdxPark, "min_battery_level_end"] = nextTripActsIdx.loc[
            multiIdxTrip, "min_battery_level_start"
        ].values
        parkActsIdx["min_battery_level_start_unlimited"] = (
            parkActsIdx.loc[multiIdxPark, "min_battery_level_end"] - parkActsIdx.loc[multiIdxPark, "max_charge_volume"]
        )
        parkActsIdx.loc[multiIdxPark, "min_battery_level_start"] = parkActsIdx["min_battery_level_start_unlimited"].where(
            parkActsIdx["min_battery_level_start_unlimited"] >= self.lower_battery_level, other=self.lower_battery_level
        )
        tmpUndershoot = parkActsIdx["min_battery_level_start_unlimited"] - self.lower_battery_level
        parkActsIdx["min_undershoot"] = tmpUndershoot.where(tmpUndershoot >= 0, other=0)
        return parkActsIdx.reset_index()

    def _uncontrolled_charging(self):
        park_activities = self.activities.loc[self.activities["trip_id"].isna(), :].copy()
        park_activities["uncontrolled_charging"] = park_activities["max_battery_level_end"] - park_activities["max_battery_level_start"]

        # Calculate timestamp at which charging ends disregarding parking end
        park_activities["timestamp_end_uncontrolled_charging_unlimited"] = park_activities.apply(
            lambda x: self._calculate_charging_end_timestamp(
                start_timestamp=x["timestamp_start"], start_battery_level=x["max_battery_level_start"], power=x["available_power"]
            ),
            axis=1,
        )

        # Take into account possible earlier disconnection due to end of parking
        park_activities["timestamp_end_uncontrolled_charging"] = park_activities["timestamp_end_uncontrolled_charging_unlimited"].where(
            park_activities["timestamp_end_uncontrolled_charging_unlimited"] <= park_activities["timestamp_end"], other=park_activities["timestamp_end"]
        )

        # This would be a neater implementation of the above, but
        # timestamp_end_uncontrolled_charging_unlimited contains NA making it impossible to convert to
        # datetime with .dt which is a prerequisite to applying
        # pandas.DataFrame.min()
        # park_activities['timestamp_end_uncontrolled_charging'] = park_activities[
        #     ['timestamp_end_uncontrolled_charging_unlimited', 'timestamp_end']].min(axis=1)

        self.activities.loc[self.activities["trip_id"].isna(), :] = park_activities

    def _calculate_charging_end_timestamp(self, start_timestamp: pd.Timestamp, start_battery_level: float, power: float) -> pd.Timestamp:
        if power == 0:
            return pd.NA
        delta_battery_level = self.upper_battery_level - start_battery_level
        time_for_charge = delta_battery_level / power  # in hours
        return start_timestamp + pd.Timedelta(value=time_for_charge, unit="h").round(freq="s")

    def _auxiliary_fuel_need(self):
        self.activities["auxiliary_fuel_need"] = (
            self.activities["residual_need"]
            * self.user_config["flexEstimators"]["fuel_consumption"]
            / self.user_config["flexEstimators"]["electric_consumption"]
        )

    def _filter_residual_need(self, activities: pd.DataFrame, index_columns: list) -> pd.DataFrame:
        """
        Filter out days (uniqueIDs) that require additional fuel, i.e. for which the trip distance cannot be
        completely be fulfilled with the available charging power. Since additional fuel for a single trip motivates
        filtering out the whole vehicle, indexCol defines the columns that make up one vehicle. If index_columns is
        ['unique_id'], all uniqueIDs that have at least one trip requiring fuel are disregarded. If index_columns is
        ['categoryID', 'weekID'] each unique combination of categoryID and weekID (each "week") for which fuel is
        required in at least one trip is disregarded.

        Args:
            activities (pd.DataFrame): Activities data set containing at least the columns 'unique_id' and 'max_residual_need'
            index_columns (list): Columns that define a "day", i.e. all unique combinations where at least one activity
                requires residual fuel are disregarded.
        """
        actsIdx = activities.set_index(index_columns)
        idxOut = (~actsIdx["max_residual_need"].isin([None, 0])) | (~actsIdx["min_residual_need"].isin([None, 0]))

        if len(index_columns) == 1:
            catWeekIDOut = actsIdx.index[idxOut]
            actsFilt = actsIdx.loc[~actsIdx.index.isin(catWeekIDOut)]
        else:
            catWeekIDOut = activities.loc[idxOut.values, index_columns]
            tplFilt = catWeekIDOut.apply(lambda x: tuple(x), axis=1).unique()
            actsFilt = actsIdx.loc[~actsIdx.index.isin(tplFilt), :]
        return actsFilt.reset_index()

    def __write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["flex_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["flex_output"]
            file_name = create_file_name(
                user_config=self.user_config,
                dev_config=self.dev_config,
                manual_label="",
                file_name_id="output_flexEstimator",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)

    def estimate_technical_flexibility_no_boundary_constraints(self) -> pd.DataFrame:
        """
        Main run function for the class WeekFlexEstimator. Calculates uncontrolled charging as well as technical
        boundary constraints for controlled charging and feeding electricity back into the grid on an indvidiual vehicle
        basis. If filter_fuel_need is True, only electrifiable days are considered.

        Returns:
            pd.DataFrame: Activities data set comprising uncontrolled charging and flexible charging constraints for
            each car.
        """
        self._drain()
        self._max_charge_volume_per_parking_activity()
        self.__battery_level_max(start_level=self.upper_battery_level * self.user_config["flexEstimators"]["start_soc"])
        self._uncontrolled_charging()
        self.__battery_level_min()
        self._auxiliary_fuel_need()
        if self.user_config["flexEstimators"]["filter_fuel_need"]:
            self.activities = self._filter_residual_need(activities=self.activities, index_columns=["unique_id"])
        if self.user_config["global"]["write_output_to_disk"]["flex_output"]:
            self.__write_output()
        print("Technical flexibility estimation ended.")
        return self.activities

    def estimate_technical_flexibility_through_iteration(self) -> pd.DataFrame:
        """
        Main run function for the class WeekFlexEstimator. Calculates uncontrolled charging as well as technical
        boundary constraints for controlled charging and feeding electricity back into the grid on an indvidiual vehicle
        basis. If filter_fuel_need is True, only electrifiable days are considered.

        Returns:
            pd.DataFrame: Activities data set comprising uncontrolled charging and flexible charging constraints for
            each car.
        """
        self._drain()
        self._max_charge_volume_per_parking_activity()
        self.__iterative_battery_level_calculation(
            max_iteration=self.user_config["flexEstimators"]["max_iterations"],
            epsilon=self.user_config["flexEstimators"]["epsilon_battery_level"],
            battery_capacity=self.user_config["flexEstimators"]["battery_capacity"],
            number_vehicles=len(self.activities["unique_id"].unique()),
        )
        self._auxiliary_fuel_need()
        if self.user_config["flexEstimators"]["filter_fuel_need"]:
            self.activities = self._filter_residual_need(activities=self.activities, index_columns=["unique_id"])
        if self.user_config["global"]["write_output_to_disk"]["flex_output"]:
            self.__write_output()
        print("Technical flexibility estimation ended.")
        return self.activities

    def __iterative_battery_level_calculation(self, max_iteration: int, epsilon: float, battery_capacity: float, number_vehicles: int):
        """
        A single iteration of calculation maximum battery levels, uncontrolled charging and minimum battery levels
        for each trip. Initial battery level for first iteration loop per unique_id in index. Start battery level will be
        set to end battery level consecutively. Function operates on class attribute self.activities.

        Args:
            max_iteration (int): Maximum iteration limit if epsilon threshold is never reached.
            epsilon (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            battery_capacity (float): Average nominal battery capacity per vehicle in kWh.
            number_vehicles (int): Number of vehicles in the empiric mobility pattern data set.
        """
        max_battery_level_end = self.upper_battery_level * self.user_config["flexEstimators"]["start_soc"]
        min_battery_level_start = self.lower_battery_level
        absolute_epsilon = int(self.__absolute_epsilon(epsilon=epsilon, battery_capacity=battery_capacity, number_vehicles=number_vehicles))

        max_battery_level_end = self.__battery_level_max(start_level=max_battery_level_end)
        self._uncontrolled_charging()
        min_battery_level_start = self.__battery_level_min(endLevel=min_battery_level_start)

        max_delta = self.__get_delta(start_column="max_battery_level_start", end_column="max_battery_level_end")
        min_delta = self.__get_delta(start_column="min_battery_level_start", end_column="min_battery_level_end")

        print(
            f"Finished iteration {1} / {max_iteration}. Delta max battery level is {int(max_delta)} / {absolute_epsilon} "
            f"and delta min battery is {int(min_delta)} / {absolute_epsilon}."
        )

        for i in range(1, max_iteration + 1):
            if max_delta < absolute_epsilon and min_delta < absolute_epsilon:
                break

            elif max_delta >= absolute_epsilon:
                max_battery_level_end = self.__battery_level_max(start_level=max_battery_level_end)
                self._uncontrolled_charging()
                max_delta = self.__get_delta(start_column="max_battery_level_start", end_column="max_battery_level_end")

            else:
                min_battery_level_start = self.__battery_level_min(endLevel=min_battery_level_start)
                min_delta = self.__get_delta(start_column="min_battery_level_start", end_column="min_battery_level_end")

            print(
                f"Finished iteration {i} / {max_iteration}. Delta max battery level is {int(max_delta)} / {absolute_epsilon} "
                f"and delta min battery is {int(min_delta)} / {absolute_epsilon}."
            )

    def __absolute_epsilon(self, epsilon: float, battery_capacity: float, number_vehicles: int) -> float:
        """
        Calculates the absolute threshold of battery level deviatiation used for interrupting the battery level
        calculation iterations.

        Args:
            epsilon (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            batteryCapacity (float): Average battery capacity per car
            number_vehicles (int): Number of vehicles

        Returns:
            float: Absolute iteration threshold in kWh of fleet battery
        """
        return epsilon * battery_capacity * number_vehicles

    def __get_delta(self, start_column: str, end_column: str) -> float:
        return abs(
            self.activities.loc[self.activities["is_last_activity"], end_column].values
            - self.activities.loc[self.activities["is_first_activity"], start_column].values
        ).sum()
