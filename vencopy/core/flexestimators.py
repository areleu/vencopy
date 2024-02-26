__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd

from pathlib import Path

from ..utils.utils import create_file_name, write_out
from ..utils.metadata import read_metadata_config, write_out_metadata


class FlexEstimator:
    def __init__(self, configs: dict, activities: pd.DataFrame):
        """
        _summary_

        Args:
            configs (dict): _description_
            activities (pd.DataFrame): _description_
        """
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.upper_battery_level = (
            self.user_config["flexestimators"]["battery_capacity"] * self.user_config["flexestimators"]["maximum_soc"]
        )
        self.lower_battery_level = (
            self.user_config["flexestimators"]["battery_capacity"] * self.user_config["flexestimators"]["minimum_soc"]
        )
        self.activities = activities.copy()
        self.is_trip = ~self.activities["trip_id"].isna()
        self.is_park = ~self.activities["park_id"].isna()
        self.is_first_activity = self.activities["is_first_activity"].fillna(0).astype(bool)
        self.is_last_activity = self.activities["is_last_activity"].fillna(0).astype(bool)

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
        """
        _summary_

        Returns:
            _type_: _description_
        """
        self.activities["drain"] = (
            self.activities["trip_distance"] * self.user_config["flexestimators"]["electric_consumption"] / 100
        )

    def _max_charge_volume_per_parking_activity(self):
        """
        _summary_
        """
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
        possible until the maximum battery capacity is reached. act_temp is the
        overall collector for each activity's park and trip results, that will
        then get written to self.activities at the very end.

        Args:
            start_level (float): Battery start level for first activity of the
            activity chain
        """
        print("Starting maximum battery level calculation.")
        print(f"Calculating maximum battery level for first activities.")

        first_activities = self._calculate_max_battery_level_first_activity(start_level=start_level)
        first_parking_activities = first_activities.loc[~first_activities["park_id"].isna(), :]

        # The second condition is needed to circumvent duplicates with tripIDs=1
        # which are initiated above
        first_trip_activities = first_activities.loc[
            (~first_activities["trip_id"].isna()) & (first_activities["is_first_activity"]), :
        ]
        act_temp = pd.concat([first_parking_activities, first_trip_activities])

        # Start and end for all trips and parkings in between
        set_acts = range(1, int(self.activities["park_id"].max()) + 1)
        subset_trip_activities = pd.DataFrame()  # Redundant?
        for act in set_acts:  # implementable via groupby with actIDs as groups?
            print(f"Calculating maximum battery level for act {act}.")
            trip_rows = (self.activities["trip_id"] == act) & (~self.activities["is_first_activity"])
            park_rows = (self.activities["park_id"] == act) & (~self.activities["is_first_activity"])
            trip_activities = self.activities.loc[trip_rows, :]
            park_activities = self.activities.loc[park_rows, :]

            # Filtering for the previous park activites that have the current activity as next activity
            previous_park_activities = act_temp.loc[
                (act_temp["next_activity_id"] == act) & (~act_temp["park_id"].isna()), :
            ]

            # first_activities trips with trip_id==0 (overnight morning splits) are
            # handled in _calculate_max_battery_level_first_activity above
            # if act == 1:
            #     subset_trip_activities = self.__calculate_max_battery_level_trip(
            #         activity_id=act, trip_activities=trip_activities, previous_park_activities=first_parking_activities
            #     )
            #     subset_park_activities = self.__calculate_max_battery_level_park(
            #         activity_id=act, park_activities=park_activities, previous_trip_activities=subset_trip_activities
            #     )
            # Park activities start off a new activity index e.g. parkAct 1 is always before tripAct 1
            subset_trip_activities = self.__calculate_max_battery_level_trip(
                activity_id=act, trip_activities=trip_activities, previous_park_activities=previous_park_activities
            )
            act_temp = pd.concat([act_temp, subset_trip_activities], ignore_index=True)
            previous_trip_activities = act_temp.loc[
                (act_temp["next_activity_id"] == act) & (~act_temp["trip_id"].isna()), :
            ]
            subset_park_activities = self.__calculate_max_battery_level_park(
                activity_id=act, park_activities=park_activities, previous_trip_activities=previous_trip_activities
            )

            act_temp = pd.concat([act_temp, subset_park_activities], ignore_index=True)
            # previous_trip_activities = subset_trip_activities  # Redundant?
        self.activities = act_temp.sort_values(by=["unique_id", "activity_id", "trip_id"])
        return self.activities.loc[
            self.activities["is_last_activity"], ["unique_id", "max_battery_level_end"]
        ].set_index("unique_id")

    def __battery_level_min(self, end_level: pd.Series) -> pd.Series:
        """
        Calculate the minimum battery level at the beginning and end of each
        activity. This represents the case of vehicles just being charged for
        the energy required for the next trip and as late as possible. The loop
        works exactly inverted to the batteryLevelMax() function since later
        trips influence the energy that has to be charged in parking activities
        before. Thus, activities are looped over from the last activity to
        first.

        Args:
            end_level (pd.Series): _description_

        Returns:
            pd.Series: _description_
        """
        print("Starting minimum battery level calculation.")
        print(f"Calculate minimum battery level for last activities.")
        last_activities = self._calculate_min_battery_level_last_activity(end_level=end_level)
        act_temp = last_activities
        # Start and end for all trips and parkings starting from the last
        # activities, then looping to earlier activities
        n_act = int(self.activities["park_id"].max())
        for act in range(n_act, -1, -1):
            print(f"Calculate minimum battery level for act {act}.")
            trip_rows = (self.activities["trip_id"] == act) & (~self.activities["is_last_activity"])
            park_rows = (self.activities["park_id"] == act) & (~self.activities["is_last_activity"])
            trip_activities = self.activities.loc[trip_rows, :]
            park_activities = self.activities.loc[park_rows, :]

            next_trip_activities = act_temp.loc[
                (act_temp["previous_activity_id"] == act) & (~act_temp["trip_id"].isna()), :
            ]
            if act != n_act:
                subset_park_activities = self.__calculate_min_battery_level_park(
                    activity_id=act, park_activities=park_activities, next_trip_activities=next_trip_activities
                )
                act_temp = pd.concat([act_temp, subset_park_activities], ignore_index=True)
            next_park_activities = act_temp.loc[
                (act_temp["previous_activity_id"] == act) & (~act_temp["park_id"].isna()), :
            ]
            subset_trip_activities = self.__calculate_min_battery_level_trip(
                activity_id=act, trip_activities=trip_activities, next_park_activities=next_park_activities
            )
            act_temp = pd.concat([act_temp, subset_trip_activities], ignore_index=True)
        self.activities = act_temp.sort_values(by=["unique_id", "activity_id", "trip_id"], ignore_index=True)
        return self.activities.loc[
            self.activities["is_first_activity"], ["unique_id", "min_battery_level_start"]
        ].set_index("unique_id")

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
        indeces = self.__get_indeces_first_activity()
        first_activities = self.activities.loc[indeces, :].copy()
        first_activities = first_activities.set_index("unique_id")
        first_activities["max_battery_level_start"] = start_level
        first_activities = first_activities.reset_index("unique_id")
        first_activities.index = first_activities.index
        first_activities = first_activities
        is_park = ~first_activities["park_id"].isna()
        is_trip = ~first_activities["trip_id"].isna()
        first_activities.loc[is_park, "max_battery_level_end_unlimited"] = (
            first_activities["max_battery_level_start"] + first_activities["max_charge_volume"]
        )
        first_activities.loc[is_park, "max_battery_level_end"] = first_activities.loc[
            is_park, "max_battery_level_end_unlimited"
        ].where(
            first_activities.loc[is_park, "max_battery_level_end_unlimited"] <= self.upper_battery_level,
            other=self.upper_battery_level,
        )
        first_activities.loc[is_park, "max_overshoot"] = (
            first_activities["max_battery_level_end_unlimited"] - first_activities["max_battery_level_end"]
        )
        first_activities.loc[is_trip, "max_battery_level_end_unlimited"] = (
            first_activities.loc[is_trip, "max_battery_level_start"] - first_activities.loc[is_trip, "drain"]
        )
        first_activities.loc[is_trip, "max_battery_level_end"] = first_activities.loc[
            is_trip, "max_battery_level_end_unlimited"
        ].where(
            first_activities.loc[is_trip, "max_battery_level_end_unlimited"] >= self.lower_battery_level,
            other=self.lower_battery_level,
        )
        res = (
            first_activities.loc[is_trip, "max_battery_level_end"]
            - first_activities.loc[is_trip, "max_battery_level_end_unlimited"]
        )
        first_activities.loc[is_trip, "max_residual_need"] = res.where(
            first_activities.loc[is_trip, "max_battery_level_end_unlimited"] < self.lower_battery_level, other=0
        )
        return first_activities

    def __get_indeces_first_activity(self) -> pd.Series:
        """
        Get indices of all activities that should be treated here. These comprise not only the first activities
        determined by the column is_first_activity but also the split-up overnight trips with the trip_id==0 and the first
        parking activities with park_id==1. This method is overwritten in the FlexEstimatorWeek.

        Returns:
            pd.Series: Boolean Series identifying the relevant rows for calculating battery levels for first activities
        """
        return self.activities["is_first_activity"]  # | (self.activities["park_id"] == 1)

    def _calculate_min_battery_level_last_activity(self, end_level: pd.Series) -> pd.DataFrame:
        """
        Calculate the minimum battery levels for the last activity in the dataset determined by the maximum activity
        ID.

        Args:
            end_level (float or pd.Series): End battery level at end of simulation time (last_bin). Defaults to
            self.lower_battery_level, the minimum battery level. Can be either of type float (in first iteration) or pd.Series
            with respective unique_id in the index.

        Returns:
            pd.DataFrame: Activity data set with the battery variables set for all last activities of the activity chains
        """
        # Last activities - parking and trips
        last_activities_in = self.activities.loc[self.activities["is_last_activity"], :].copy()
        is_trip = ~last_activities_in["trip_id"].isna()

        indeces_last_activities = last_activities_in.set_index("unique_id")
        indeces_last_activities["min_battery_level_end"] = end_level
        indeces_last_activities.loc[
            indeces_last_activities["trip_id"].isna(), "min_battery_level_start"
        ] = end_level  # For park activities

        last_activities = indeces_last_activities.reset_index("unique_id")
        last_activities.index = last_activities_in.index

        last_activities.loc[is_trip, "min_battery_level_start_unlimited"] = (
            last_activities.loc[is_trip, "min_battery_level_end"] + last_activities.loc[is_trip, "drain"]
        )
        last_activities.loc[is_trip, "min_battery_level_start"] = last_activities.loc[
            is_trip, "min_battery_level_start_unlimited"
        ].where(
            last_activities.loc[is_trip, "min_battery_level_start_unlimited"] <= self.upper_battery_level,
            other=self.upper_battery_level,
        )
        residual_need = last_activities.loc[is_trip, "min_battery_level_start_unlimited"] - self.upper_battery_level
        last_activities.loc[is_trip, "residual_need"] = residual_need.where(residual_need >= 0, other=0)
        return last_activities

    def __calculate_max_battery_level_trip(
        self, activity_id: int, trip_activities: pd.DataFrame, previous_park_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        _summary_

        Args:
            activity_id (int): _description_
            trip_activities (pd.DataFrame): _description_
            previous_park_activities (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: _description_
        """
        # Setting trip activity battery start level to battery end level of previous parking
        # Index setting of trip activities to be updated
        active_unique_ids = trip_activities.loc[:, "unique_id"]
        multi_index_trip = [(id, activity_id, None) for id in active_unique_ids]
        indeces_trip_activities = trip_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Index setting of previous park activities as basis for the update
        previous_park_ids = trip_activities.loc[:, "previous_activity_id"]
        multi_index_park = [(id, None, act) for id, act in zip(active_unique_ids, previous_park_ids)]
        indeces_previous_park_activities = previous_park_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Calculation of battery level at start and end of trip
        indeces_trip_activities.loc[multi_index_trip, "max_battery_level_start"] = indeces_previous_park_activities.loc[
            multi_index_park, "max_battery_level_end"
        ].values
        indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end_unlimited"] = (
            indeces_trip_activities.loc[multi_index_trip, "max_battery_level_start"]
            - indeces_trip_activities.loc[multi_index_trip, "drain"]
        )
        indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end"] = indeces_trip_activities.loc[
            multi_index_trip, "max_battery_level_end_unlimited"
        ].where(
            indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end_unlimited"]
            >= self.lower_battery_level,
            other=self.lower_battery_level,
        )
        res = (
            indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end"]
            - indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end_unlimited"]
        )
        indeces_trip_activities.loc[multi_index_trip, "max_residual_need"] = res.where(
            indeces_trip_activities.loc[multi_index_trip, "max_battery_level_end_unlimited"] < self.lower_battery_level,
            other=0,
        )
        return indeces_trip_activities.reset_index()

    def __calculate_min_battery_level_trip(
        self, activity_id: int, trip_activities: pd.DataFrame, next_park_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        _summary_

        Args:
            activity_id (int): _description_
            trip_activities (pd.DataFrame): _description_
            next_park_activities (pd.DataFrame, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: _description_
        """
        # Setting trip activity battery start level to battery end level of previous parking
        active_unique_ids = trip_activities.loc[:, "unique_id"]
        multi_index_trip = [(id, activity_id, None) for id in active_unique_ids]
        # Index the previous park activity via integer index because loc park indices vary
        indeces_trip_activities = trip_activities.set_index(["unique_id", "trip_id", "park_id"])

        next_park_ids = trip_activities.loc[:, "next_activity_id"]
        multi_index_park = [(id, None, act) for id, act in zip(active_unique_ids, next_park_ids)]
        indeces_next_park_activities = next_park_activities.set_index(["unique_id", "trip_id", "park_id"])
        indeces_trip_activities.loc[multi_index_trip, "min_battery_level_end"] = indeces_next_park_activities.loc[
            multi_index_park, "min_battery_level_start"
        ].values

        # Setting minimum battery end level for trip
        indeces_trip_activities.loc[multi_index_trip, "min_battery_level_start_unlimited"] = (
            indeces_trip_activities.loc[multi_index_trip, "min_battery_level_end"]
            + indeces_trip_activities.loc[multi_index_trip, "drain"]
        )
        indeces_trip_activities.loc[multi_index_trip, "min_battery_level_start"] = indeces_trip_activities.loc[
            multi_index_trip, "min_battery_level_start_unlimited"
        ].where(
            indeces_trip_activities.loc[multi_index_trip, "min_battery_level_start_unlimited"]
            <= self.upper_battery_level,
            other=self.upper_battery_level,
        )
        residual_need = (
            indeces_trip_activities.loc[multi_index_trip, "min_battery_level_start_unlimited"]
            - self.upper_battery_level
        )
        indeces_trip_activities.loc[multi_index_trip, "min_residual_need"] = residual_need.where(
            residual_need >= 0, other=0
        )
        return indeces_trip_activities.reset_index()

    def __calculate_max_battery_level_park(
        self, activity_id: int, park_activities: pd.DataFrame, previous_trip_activities: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Calculate the maximum battery level of the given parking activities for the activity ID given by activity_id. Previous trip
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
        multi_index_park = [(id, None, activity_id) for id in active_unique_ids]
        indeces_park_activities = park_activities.set_index(["unique_id", "trip_id", "park_id"])

        # Index setting of previous trip activities used to update
        previous_trip_ids = park_activities.loc[:, "previous_activity_id"]
        multi_index_trip = [(id, act, None) for id, act in zip(active_unique_ids, previous_trip_ids)]
        indeces_previous_trip_activities = previous_trip_activities.set_index(["unique_id", "trip_id", "park_id"])

        # Calculation of battery level at start and end of park activity
        indeces_park_activities.loc[multi_index_park, "max_battery_level_start"] = indeces_previous_trip_activities.loc[
            multi_index_trip, "max_battery_level_end"
        ].values
        indeces_park_activities["max_battery_level_end_unlimited"] = (
            indeces_park_activities.loc[multi_index_park, "max_battery_level_start"]
            + indeces_park_activities.loc[multi_index_park, "max_charge_volume"]
        )
        indeces_park_activities.loc[multi_index_park, "max_battery_level_end"] = indeces_park_activities[
            "max_battery_level_end_unlimited"
        ].where(
            indeces_park_activities["max_battery_level_end_unlimited"] <= self.upper_battery_level,
            other=self.upper_battery_level,
        )
        temporary_overshoot = indeces_park_activities["max_battery_level_end_unlimited"] - self.upper_battery_level
        indeces_park_activities["max_overshoot"] = temporary_overshoot.where(temporary_overshoot >= 0, other=0)
        return indeces_park_activities.reset_index()

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
        multi_index_park = [(id, None, activity_id) for id in active_unique_ids]
        indeces_park_activities = park_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Composing trip activity index to get battery level from
        next_trip_ids = park_activities.loc[:, "next_activity_id"]
        multi_index_trip = [(id, act, None) for id, act in zip(active_unique_ids, next_trip_ids)]
        indeces_next_trip_activities = next_trip_activities.set_index(["unique_id", "trip_id", "park_id"])
        # Setting next park activity battery start level to battery end level of current trip
        indeces_park_activities.loc[multi_index_park, "min_battery_level_end"] = indeces_next_trip_activities.loc[
            multi_index_trip, "min_battery_level_start"
        ].values
        indeces_park_activities["min_battery_level_start_unlimited"] = (
            indeces_park_activities.loc[multi_index_park, "min_battery_level_end"]
            - indeces_park_activities.loc[multi_index_park, "max_charge_volume"]
        )
        indeces_park_activities.loc[multi_index_park, "min_battery_level_start"] = indeces_park_activities[
            "min_battery_level_start_unlimited"
        ].where(
            indeces_park_activities["min_battery_level_start_unlimited"] >= self.lower_battery_level,
            other=self.lower_battery_level,
        )
        temporary_undershoot = indeces_park_activities["min_battery_level_start_unlimited"] - self.lower_battery_level
        indeces_park_activities["min_undershoot"] = temporary_undershoot.where(temporary_undershoot >= 0, other=0)
        return indeces_park_activities.reset_index()

    def _uncontrolled_charging(self):
        """
        _summary_
        """
        park_activities = self.activities.loc[self.activities["trip_id"].isna(), :].copy()
        park_activities["uncontrolled_charging"] = (
            park_activities["max_battery_level_end"] - park_activities["max_battery_level_start"]
        )

        # Calculate timestamp at which charging ends disregarding parking end
        park_activities["timestamp_end_uncontrolled_charging_unlimited"] = park_activities.apply(
            lambda x: self._calculate_charging_end_timestamp(
                start_timestamp=x["timestamp_start"],
                start_battery_level=x["max_battery_level_start"],
                power=x["available_power"],
            ),
            axis=1,
        )

        # Take into account possible earlier disconnection due to end of parking
        park_activities["timestamp_end_uncontrolled_charging"] = park_activities[
            "timestamp_end_uncontrolled_charging_unlimited"
        ].where(
            park_activities["timestamp_end_uncontrolled_charging_unlimited"] <= park_activities["timestamp_end"],
            other=park_activities["timestamp_end"],
        )
        # This would be a neater implementation of the above, but
        # timestamp_end_uncontrolled_charging_unlimited contains NA making it impossible to convert to
        # datetime with .dt which is a prerequisite to applying
        # pandas.DataFrame.min()
        # park_activities['timestamp_end_uncontrolled_charging'] = park_activities[
        #     ['timestamp_end_uncontrolled_charging_unlimited', 'timestamp_end']].min(axis=1)
        self.activities.loc[self.activities["trip_id"].isna(), :] = park_activities

    def _calculate_charging_end_timestamp(
        self, start_timestamp: pd.Timestamp, start_battery_level: float, power: float
    ) -> pd.Timestamp:
        """
        _summary_

        Args:
            start_timestamp (pd.Timestamp): _description_
            start_battery_level (float): _description_
            power (float): _description_

        Returns:
            pd.Timestamp: _description_
        """
        if power == 0:
            return pd.NA
        delta_battery_level = self.upper_battery_level - start_battery_level
        time_for_charge = delta_battery_level / power  # in hours
        return start_timestamp + pd.Timedelta(value=time_for_charge, unit="h").round(freq="s")

    def _auxiliary_fuel_need(self):
        """
        _summary_
        """
        self.activities["auxiliary_fuel_need"] = (
            self.activities["residual_need"]
            * self.user_config["flexestimators"]["fuel_consumption"]
            / self.user_config["flexestimators"]["electric_consumption"]
        )

    def _filter_residual_need(self, activities: pd.DataFrame, index_columns: list) -> pd.DataFrame:
        """
        Filter out days (uniqueIDs) that require additional fuel, i.e. for which the trip distance cannot be
        completely be fulfilled with the available charging power. Since additional fuel for a single trip motivates
        filtering out the whole vehicle, index_columns defines the columns that make up one vehicle. If index_columns is
        ['unique_id'], all uniqueIDs that have at least one trip requiring fuel are disregarded. If index_columns is
        ['category_id', 'week_id'] each unique combination of category_id and week_id (each "week") for which fuel is
        required in at least one trip is disregarded.

        Args:
            activities (pd.DataFrame): Activities data set containing at least the columns 'unique_id' and 'max_residual_need'
            index_columns (list): Columns that define a "day", i.e. all unique combinations where at least one activity
                requires residual fuel are disregarded.
        """
        indeces_activities = activities.set_index(index_columns)
        indeces_out = (~indeces_activities["max_residual_need"].isin([None, 0])) | (
            ~indeces_activities["min_residual_need"].isin([None, 0])
        )

        if len(index_columns) == 1:
            category_week_ids_out = indeces_activities.index[indeces_out]
            activities_filter = indeces_activities.loc[~indeces_activities.index.isin(category_week_ids_out)]
        else:
            category_week_ids_out = activities.loc[indeces_out.values, index_columns]
            filter = category_week_ids_out.apply(lambda x: tuple(x), axis=1).unique()
            activities_filter = indeces_activities.loc[~indeces_activities.index.isin(filter), :]
        return activities_filter.reset_index()

    def __write_output(self):
        """
        _summary_
        """
        if self.user_config["global"]["write_output_to_disk"]["flex_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["flex_output"]
            file_name = create_file_name(
                user_config=self.user_config,
                dev_config=self.dev_config,
                file_name_id="output_flexestimator",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)
            self._write_metadata(file_name=root / folder / file_name)

    def generate_metadata(self, metadata_config, file_name):
        metadata_config["name"] = file_name
        metadata_config["title"] = "National Travel Survey activities dataframe"
        metadata_config["description"] = "Trips and parking activities from venco.py including profiles representing the available charging power, an uncontrolled charging profile, the battery drain, and the maximum and minum battery level."
        metadata_config["sources"] = [f for f in metadata_config["sources"] if f["title"] in self.dataset]
        reference_resource = metadata_config["resources"][0]
        this_resource = reference_resource.copy()
        this_resource["name"] = file_name.rstrip(".csv")
        this_resource["path"] = file_name
        these_fields = [f for f in reference_resource["schema"][self.dataset]["fields"]["flexestimators"] if f["name"] in self.activities.columns]
        this_resource["schema"] = {"fields": these_fields}
        metadata_config["resources"].pop()
        metadata_config["resources"].append(this_resource)
        return metadata_config

    def _write_metadata(self, file_name):
        metadata_config = read_metadata_config()
        class_metadata = self.generate_metadata(metadata_config=metadata_config, file_name=file_name.name)
        write_out_metadata(metadata_yaml=class_metadata, file_name=file_name.as_posix().replace(".csv", ".metadata.yaml"))

    def __iterative_battery_level_calculation(
        self, max_iteration: int, epsilon: float, battery_capacity: float, number_vehicles: int
    ):
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
        max_battery_level_end = self.upper_battery_level * self.user_config["flexestimators"]["start_soc"]
        min_battery_level_start = self.lower_battery_level
        absolute_epsilon = int(
            self.__absolute_epsilon(epsilon=epsilon, battery_capacity=battery_capacity, number_vehicles=number_vehicles)
        )

        max_battery_level_end = self.__battery_level_max(start_level=max_battery_level_end)
        self._uncontrolled_charging()
        min_battery_level_start = self.__battery_level_min(end_level=min_battery_level_start)

        max_delta = self.__get_delta(start_column="max_battery_level_start", end_column="max_battery_level_end")
        min_delta = self.__get_delta(start_column="min_battery_level_start", end_column="min_battery_level_end")

        print(
            f"Finished iteration {1} / {max_iteration}. Delta max battery level is {int(max_delta)}, delta min "
            f"battery level is {int(min_delta)} and threshold epsilon is {absolute_epsilon}."
        )

        for i in range(1, max_iteration + 1):
            if max_delta < absolute_epsilon and min_delta < absolute_epsilon:
                break

            elif max_delta >= absolute_epsilon:
                max_battery_level_end = self.__battery_level_max(start_level=max_battery_level_end)
                self._uncontrolled_charging()
                max_delta = self.__get_delta(start_column="max_battery_level_start", end_column="max_battery_level_end")

            else:
                min_battery_level_start = self.__battery_level_min(end_level=min_battery_level_start)
                min_delta = self.__get_delta(start_column="min_battery_level_start", end_column="min_battery_level_end")

            print(
                f"Finished iteration {i} / {max_iteration}. Delta max battery level is {int(max_delta)}, delta min "
                f"battery level is {int(min_delta)} and threshold epsilon is {absolute_epsilon}."
            )

    def __absolute_epsilon(self, epsilon: float, battery_capacity: float, number_vehicles: int) -> float:
        """
        Calculates the absolute threshold of battery level deviatiation (delta in kWh for the whole fleet)
        used for interrupting the battery level calculation iterations.

        Args:
            epsilon (float): Share of total aggregated battery fleet capacity (e.g. 0.01 for 1% would relate to a threshold of 100 Wh per car for a 10 kWh battery capacity.)
            batteryCapacity (float): Average battery capacity per car
            number_vehicles (int): Number of vehicles

        Returns:
            float: Absolute iteration threshold in kWh of fleet battery
        """
        return epsilon * battery_capacity * number_vehicles

    def __get_delta(self, start_column: str, end_column: str) -> float:
        """
        _summary_

        Args:
            start_column (str): _description_
            end_column (str): _description_

        Returns:
            float: _description_
        """
        delta = abs(
            self.activities.loc[self.activities["is_last_activity"], end_column].values
            - self.activities.loc[self.activities["is_first_activity"], start_column].values
        ).sum()
        return delta

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
        self.__battery_level_max(start_level=self.upper_battery_level * self.user_config["flexestimators"]["start_soc"])
        self._uncontrolled_charging()
        self.__battery_level_min()
        self._auxiliary_fuel_need()
        if self.user_config["flexestimators"]["filter_fuel_need"]:
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
            max_iteration=self.user_config["flexestimators"]["max_iterations"],
            epsilon=self.user_config["flexestimators"]["epsilon_battery_level"],
            battery_capacity=self.upper_battery_level,
            number_vehicles=len(self.activities["unique_id"].unique()),
        )
        self._auxiliary_fuel_need()
        if self.user_config["flexestimators"]["filter_fuel_need"]:
            self.activities = self._filter_residual_need(activities=self.activities, index_columns=["unique_id"])
        if self.user_config["global"]["write_output_to_disk"]["flex_output"]:
            self.__write_output()
        print("Technical flexibility estimation ended.")
        return self.activities
