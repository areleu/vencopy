__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd

from ...utils.utils import replace_vec


class ParkInference:
    def __init__(self, configs) -> None:
        self.user_config = configs["user_config"]
        self.activities = None
        self.activities_raw = None
        self.overnight_splitter = OvernightSplitter()

    def add_parking_rows(self, trips: pd.DataFrame) -> pd.DataFrame:
        """
        Wrapper function generating park activity rows between the trip data from the original MID dataset. Some
        utility attributes are being added such as is_first_activity, is_last_activity or the unique_id of the next and
        previous activity. Redundant time observations are dropped after timestamp creation for start and end time of
        each activity. Overnight trips (e.g. extending from 23:00 at survey day to 1:30 on the consecutive day) are
        split up into two trips. The first one extends to the end of the day (00:00) and the other one is appended
        to the activity list before the first parking activity (0:00-1:30). The trip distance is split between the two
        based on the time.

        :param split_overnight_trips: Should trips that end on the consecutive day (not the survey day) be split in two
        trips in such a way that the estimated trip distance the next day is appended in the morning hours of the survey
        day?
        """
        self.trips = trips
        split_overnight_trips = self.user_config["dataparsers"]["split_overnight_trips"]
        self.activities_raw = self.__copy_rows(trips=self.trips)
        self.activities_raw = self.__add_util_attributes(activities_raw=self.activities_raw)
        # self.__add_park_act_after_last_trip()
        self.__add_park_act_before_first_trip()
        self.__adjust_park_attrs()
        self._drop_redundant_columns(dataset=self.activities_raw)
        self.__remove_next_day_park_acts()
        self.__adjust_park_timestamps()
        # self.__set_trip_attrs_na_for_park_acts()
        self.__add_next_and_prev_ids()
        self.__overnight_split_decider(split=split_overnight_trips)
        self.__add_timedelta_column()
        self.__unique_indeces()
        print(
            f'Finished activity composition with {self.activities_raw["trip_id"].fillna(0).astype(bool).sum()} trips '
            f'and {self.activities_raw["park_id"].fillna(0).astype(bool).sum()} parking activites.'
        )
        return self.activities_raw

    @staticmethod
    def __copy_rows(trips):
        """
        Adds skeleton duplicate rows for parking activities.
        """
        activities_raw = pd.concat([trips] * 2).sort_index(ignore_index=True)
        activities_raw["park_id"] = activities_raw["trip_id"]
        activities_raw.loc[range(1, len(activities_raw), 2), "trip_id"] = pd.NA
        activities_raw.loc[range(0, len(activities_raw), 2), "park_id"] = pd.NA
        return activities_raw

    @staticmethod
    def __add_util_attributes(activities_raw):
        """
        Adding additional attribute columns with previous and next unique_id.
        """
        activities_raw["previous_unique_id"] = activities_raw["unique_id"].shift(fill_value=0)
        activities_raw["is_first_activity"] = (
            activities_raw["previous_unique_id"] != activities_raw["unique_id"]
        )
        activities_raw["next_unique_id"] = activities_raw["unique_id"].shift(-1, fill_value=0)
        activities_raw["is_last_activity"] = (
            activities_raw["next_unique_id"] != activities_raw["unique_id"]
        )
        return activities_raw

    def __add_park_act_before_first_trip(self):
        """
        Adds park activities before first trips. Currently, it is assumed that all cars start home.
        """
        new_indeces = self.activities_raw.index[self.activities_raw["is_first_activity"]]

        df_add = self.activities_raw.loc[new_indeces, :]
        df_add["park_id"] = 0

        # This most likely throws a warning in the concat in the last line
        # df_add["trip_id"] = pd.NA

        df_add["purpose_string"] = "HOME"  # Assumption
        self.activities_raw.loc[new_indeces, "is_first_activity"] = False
        self.activities_raw = pd.concat([self.activities_raw, df_add]).sort_index()

        self.activities_raw.loc[
            (self.activities_raw["is_first_activity"]) & (self.activities_raw["park_id"] == 0), "trip_id"
        ] = pd.NA

    # DEPRECATED
    def __add_park_act_after_last_trip(self):
        """
        Adds park activities after last trips.
        """
        new_indeces = self.activities_raw.index[self.activities_raw.is_last_activity]
        df_add = self.activities_raw.loc[new_indeces, :]
        df_add["trip_id"] = pd.NA
        self.activities_raw.loc[new_indeces, "is_last_activity"] = False
        df_add["park_id"] = self.activities_raw.loc[new_indeces, "trip_id"] + 1
        self.activities_raw = pd.concat([self.activities_raw, df_add]).sort_index()

    def __adjust_park_attrs(self):
        """
        Sets trip attribute values to zero where trip_id == NaN (i.e. for all parking activities).
        """
        self.activities_raw.loc[
            self.activities_raw["trip_id"].isna(),
            ["trip_distance", "travel_time", "trip_is_intermodal"],
        ] = pd.NA
        self.activities_raw["column_from_index"] = self.activities_raw.index
        self.activities_raw = self.activities_raw.sort_values(by=["column_from_index", "park_id", "trip_id"])

    @staticmethod
    def _drop_redundant_columns(dataset):
        """
        Removes temporary redundant columns.
        """
        dataset.drop(
            columns=[
                "trip_start_clock",
                "trip_end_clock",
                "trip_start_year",
                "trip_start_month",
                "trip_start_week",
                "trip_start_hour",
                "trip_start_minute",
                "trip_end_hour",
                "trip_end_minute",
                "previous_unique_id",
                "next_unique_id",
                "column_from_index",
            ],
            inplace=True,
        )
        return dataset

    def __remove_next_day_park_acts(self):
        """
        Checks for trips across day-limit and removing respective parking activities.
        """
        # DEPRECATED
        # indeces_overnight = self.activities_raw["is_last_activity"] & self.activities_raw["trip_end_next_day"]
        # indeces_overnight = indeces_overnight.loc[indeces_overnight]
        # self.activities_raw = self.activities_raw.reset_index()

        # Get rid of park activities after overnight trips
        indeces_multi_day_activities = (
            self.activities_raw["is_last_activity"]
            & self.activities_raw["trip_end_next_day"]
            & self.activities_raw["park_id"]
        )
        uids = self.activities_raw.loc[indeces_multi_day_activities, "unique_id"]
        trip_ids = (
            self.activities_raw.loc[self.activities_raw["unique_id"].isin(uids), ["unique_id", "trip_id"]]
            .groupby(by=["unique_id"])
            .max()
        )
        idx = [(i, trip_ids.loc[i].values[0]) for i in trip_ids.index]

        self.activities_raw = self.activities_raw.loc[~indeces_multi_day_activities, :]
        acts = self.activities_raw.copy().set_index(["unique_id", "trip_id"], drop=True)
        acts.loc[idx, "is_last_activity"] = True
        self.activities_raw = acts.reset_index()

    def __adjust_park_timestamps(self):
        """
        Adjust the start and end timestamps of the newly added rows. This is done via range index, that is reset at
        the beginning. First and last activities have to be treated separately since their dates have to match with
        their daily activity chain.
        """
        self.activities_raw = self.activities_raw.reset_index()
        park_act_wo_first, park_act_wo_last = self.__get_park_acts_wo_first_and_last()
        self.__update_park_start(park_act_wo_first=park_act_wo_first)
        self.__update_park_end(park_act_wo_last=park_act_wo_last)
        self.__update_timestamp_first_park_act()
        self.__update_timestamp_last_park_act()
        print("Completed park timestamp adjustments.")

    def __get_park_acts_wo_first_and_last(self) -> pd.DataFrame:
        """
        Returns all parking activities except for the last one (return argument 1) and the first one (return argument
        2)

        Return:
            pd.Series: Parking activity indices without the last one
            pd.Series: Parking activity indices without the first one
        """
        park_act = ~self.activities_raw["park_id"].isna()
        park_act = park_act.loc[park_act]
        return park_act.iloc[1:], park_act.iloc[:-1]

    def __update_park_start(self, park_act_wo_first: pd.Series):
        """
        Updates park start timestamps for newly added rows
        """
        set_timestamp = self.activities_raw.loc[park_act_wo_first.index - 1, "timestamp_end"]
        set_timestamp.index = self.activities_raw.loc[park_act_wo_first.index, "timestamp_start"].index
        self.activities_raw.loc[park_act_wo_first.index, "timestamp_start"] = set_timestamp

    def __update_park_end(self, park_act_wo_last: pd.Series):
        """
        Updates park end timestamps for newly added rows
        """
        set_timestamp = self.activities_raw.loc[park_act_wo_last.index + 1, "timestamp_start"]
        set_timestamp.index = self.activities_raw.loc[park_act_wo_last.index, "timestamp_end"].index
        self.activities_raw.loc[park_act_wo_last.index, "timestamp_end"] = set_timestamp

    def __update_timestamp_first_park_act(self):
        """
        Updates park end timestamps for last activity in new park rows
        """
        indeces_activities = ~(self.activities_raw["park_id"].isna()) & (self.activities_raw["is_first_activity"])
        self.activities_raw.loc[indeces_activities, "timestamp_start"] = replace_vec(
            self.activities_raw.loc[indeces_activities, "timestamp_end"], hour=0, minute=0
        )

    def __update_timestamp_last_park_act(self):
        """
        Updates park end timestamps for last activity in new park rows
        """
        indeces_activities = ~(self.activities_raw["park_id"].isna()) & (self.activities_raw["is_last_activity"])
        self.activities_raw.loc[indeces_activities, "timestamp_end"] = replace_vec(
            self.activities_raw.loc[indeces_activities, "timestamp_start"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")

    def __set_trip_attrs_na_for_park_acts(self):
        """
        Sets trip_end_next_day to False for all park activities.
        """
        self.activities_raw.loc[
            self.activities_raw["trip_id"].isna(),
            [
                "trip_end_next_day",
                "trip_id",
                "trip_distance",
                "travel_time",
                "trip_is_intermodal",
            ],
        ] = pd.NA

    def __add_next_and_prev_ids(self):
        self.activities_raw.loc[~self.activities_raw["trip_id"].isna(), "activity_id"] = self.activities_raw["trip_id"]
        self.activities_raw.loc[~self.activities_raw["park_id"].isna(), "activity_id"] = self.activities_raw["park_id"]
        self.activities_raw.loc[~self.activities_raw["is_last_activity"], "next_activity_id"] = self.activities_raw.loc[
            :, "activity_id"
        ].shift(-1)
        self.activities_raw.loc[
            ~self.activities_raw["is_first_activity"], "previous_activity_id"
        ] = self.activities_raw.loc[:, "activity_id"].shift(1)

    def __overnight_split_decider(self, split: bool):
        """
        Boolean function that differentiates if overnight trips should be split (split==True) or not (split==False).
        In the latter case, overnight trips identified by the variable 'trip_end_next_day' are excluded from the data set.

        Args:
            split (bool): Should trips that end on the consecutive day (not the survey day) be split in two trips in
            such a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        if split:
            self.activities_raw = self.overnight_splitter.split_overnight_trips(trips=self.activities_raw)
        else:
            self.__set_overnight_var_false_for_last_act_trip()
            self.__neglect_overnight_trips()

    def __set_overnight_var_false_for_last_act_trip(self):
        """
        This function treats the edge case of trips being the last activity in the daily activity chain, i.e. trips
        ending exactly at 00:00. They are falsely labelled as overnight trips which is corrected here.
        """
        indeces_last_activity_is_trip = (self.activities_raw["is_last_activity"]) & ~(
            self.activities_raw["trip_id"].isna()
        )
        idx_last_trip_end_midnight = (
            indeces_last_activity_is_trip
            & (self.activities_raw.loc[indeces_last_activity_is_trip, "timestamp_end"].dt.hour == 0)
            & (self.activities_raw.loc[indeces_last_activity_is_trip, "timestamp_end"].dt.minute == 0)
        )
        self.trips_end_next_day_raw = self.activities_raw["trip_end_next_day"]
        self.activities_raw.loc[idx_last_trip_end_midnight, "trip_end_next_day"] = False

    def __neglect_overnight_trips(self):
        """
        Removes all overnight trips from the activities data set based on the column 'trip_end_next_day'. Updates
        timestamp end (to 00:00) and is_last_activity for the new last parking activities. Overwrites
        self.activities_raw.
        """
        # Column for lastActivity setting later
        self.activities_raw["next_trip_end_next_day"] = self.activities_raw["trip_end_next_day"].shift(
            -1, fill_value=False
        )

        # Get rid of overnight trips
        indeces_no_overnight_trip = ~(self.activities_raw["trip_end_next_day"].fillna(False))
        self.activities_raw = self.activities_raw.loc[indeces_no_overnight_trip, :]

        # Update is_last_activity and timestamp_end variables and clean-up column
        indeces_new_last_activity = self.activities_raw["next_trip_end_next_day"]
        indeces_new_last_activity = indeces_new_last_activity.fillna(False).astype(bool)
        self.activities_raw.loc[indeces_new_last_activity, "is_last_activity"] = True
        self.activities_raw.loc[indeces_new_last_activity, "timestamp_end"] = replace_vec(
            self.activities_raw.loc[indeces_new_last_activity, "timestamp_start"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")
        self.activities_raw = self.activities_raw.drop(columns=["next_trip_end_next_day"])

    def __add_timedelta_column(self):
        """
        Adds column keeping the length information of the activity as a pandas timeDelta.
        """
        self.activities_raw["time_delta"] = (
            self.activities_raw["timestamp_end"] - self.activities_raw["timestamp_start"]
        )

    def __unique_indeces(self):
        self.activities_raw.drop(columns=["index"], inplace=True)
        self.activities_raw.reset_index(inplace=True)  # Due to copying and appending rows, the index has to be reset


class OvernightSplitter:
    def __init__(self):
        self.activities_raw = None

    def split_overnight_trips(self, trips: pd.DataFrame) -> pd.DataFrame:
        """
        Wrapper function for treating edge case trips ending not in the 24 hours of the survey day but stretch
        to the next day. Those overnight are split up into an evening trip at the regular survey day and a
        morning trip at the next day. Trip distances are split according to the time the person spent on that trip.
        E.g. if a trip lasts from 23:00 to 2:00 the next day and 100 km, the split-up evening trip will last from
        23:00 to 00:00 of the survey day and 33 km and the morning trip from 00:00 to 2:00 and 66 km. In a next step,
        the morning trip is appended to the survey day in the first hours.

        Here, different edge cases occur.
        Edge case 1 (N=5 in MiD17): For trips that overlap with night (early morning) trips at the survey day, e.g. from
        0:30 to 1:00 for the above mentioned example, the morning part of the split overnight trip is completely
        disregarded.
        Edge case 2 (N=3 in MiD17): When overnight mornging split-trips end exactly at the time where the first trip of
        the survey day starts (2:00 in the example), both trips are consolidated to one trip with all attributes of the
        survey trip.
        These edge cases are documented and quantified in issue #358 'Sum of all distances of dataParser at end equals
        sum of all distances after filtering'.
        """
        self.activities_raw = trips

        # Split overnight trips and add next day distance in the morning (trip_id=0)
        is_overnight_trip, overnight_trips_add = self.__get_overnight_activities()
        ON_trips_add_timestamp = self.__adjust_overnight_timestamps(trips=overnight_trips_add)
        self.__set_last_activities_end_timestamp_to_zero()
        morning_trips = self.__set_overnight_trip_id_to_zero(trips=ON_trips_add_timestamp)
        morning_trips = self.__adjust_morning_trip_distance(
            overnightTrips=overnight_trips_add, morning_trips=morning_trips
        )
        self.__adjust_evening_trip_distance(morning_trips=morning_trips, is_overnight_trip=is_overnight_trip)
        self.__set_first_last_acts(morning_trips=morning_trips)
        is_prev_first_acts = self.__get_prev_first_act(
            morning_trips=morning_trips
        )  # Activities that were previously first_activities
        morning_trips_no_overlap, is_prev_first_acts = self.__neglect_overlap_morning_trips(
            morning_trips=morning_trips, is_prev_first_acts=is_prev_first_acts
        )
        morning_trips_to_add = self.__set_next_parking_timestamp_start(
            morning_trips=morning_trips_no_overlap,
            is_overnight_trip=is_overnight_trip,
            is_prev_first_acts=is_prev_first_acts,
        )
        self.__add_morning_trips(morning_trips=morning_trips_to_add)
        self.__remove_first_parking_act()
        self.__merge_adjacent_trips()
        # Implement DELTA mileage check of overnight morning split trip distances
        self.__check_and_assert()
        self.__sort_activities()

        return self.activities_raw

    def __get_overnight_activities(self) -> tuple[pd.Series, pd.DataFrame]:
        indeces_overnight_actvities = (
            self.activities_raw["is_last_activity"]
            & self.activities_raw["trip_end_next_day"]
            & ~(
                (self.activities_raw["timestamp_end"].dt.hour == 0)
                & (self.activities_raw["timestamp_end"].dt.minute == 0)  # assure that the overnight trip does
            )
        )  # not exactly end at 00:00
        return indeces_overnight_actvities, self.activities_raw.loc[indeces_overnight_actvities, :]

    def __adjust_overnight_timestamps(self, trips: pd.DataFrame) -> pd.DataFrame:
        tripsRes = trips.copy()
        tripsRes["timestamp_end"] = tripsRes.loc[:, "timestamp_end"] - pd.Timedelta(1, "d")
        tripsRes["timestamp_start"] = replace_vec(tripsRes.loc[:, "timestamp_end"], hour=0, minute=0)
        return tripsRes

    def __set_last_activities_end_timestamp_to_zero(self):
        # Set timestamp end of evening part of overnight trip split to 00:00
        self.activities_raw.loc[self.activities_raw["is_last_activity"], "timestamp_end"] = replace_vec(
            self.activities_raw.loc[self.activities_raw["is_last_activity"], "timestamp_end"],
            hour=0,
            minute=0,
        )

    def __set_overnight_trip_id_to_zero(self, trips: pd.DataFrame) -> pd.DataFrame:
        trips["trip_id"] = 0
        trips["activity_id"] = 0
        trips["previous_activity_id"] = pd.NA

        # Update next activity ID
        unique_id = trips["unique_id"]
        act_idx = self.activities_raw["unique_id"].isin(unique_id) & self.activities_raw["is_first_activity"]
        trips["next_activity_id"] = self.activities_raw.loc[act_idx, "activity_id"]

        # Update previous activity ID of previously first activity
        self.activities_raw.loc[act_idx, "previous_activity_id"] = 0
        return trips

    def __adjust_morning_trip_distance(self, overnightTrips: pd.DataFrame, morning_trips: pd.DataFrame) -> pd.DataFrame:
        # Splitting the total distance to morning and evening trip time-share dependent
        morning_trips["timedelta_total"] = overnightTrips["timestamp_end"] - overnightTrips["timestamp_start"]
        morning_trips["timedelta_morning"] = morning_trips["timestamp_end"] - morning_trips["timestamp_start"]
        morning_trips["time_share_morning"] = morning_trips["timedelta_morning"] / morning_trips["timedelta_total"]
        morning_trips["time_share_evening"] = (
            morning_trips["timedelta_total"] - morning_trips["timedelta_morning"]
        ) / morning_trips["timedelta_total"]
        morning_trips["total_trip_distance"] = morning_trips["trip_distance"]
        morning_trips["trip_distance"] = morning_trips["time_share_morning"] * morning_trips["total_trip_distance"]
        return morning_trips

    def __adjust_evening_trip_distance(self, morning_trips: pd.DataFrame, is_overnight_trip: pd.Series):
        self.activities_raw.loc[is_overnight_trip, "trip_distance"] = (
            morning_trips["time_share_evening"] * morning_trips["total_trip_distance"]
        )

    def __set_first_last_acts(self, morning_trips: pd.DataFrame):
        # Setting first and last activities
        morning_trips["is_first_activity"] = True
        morning_trips["is_last_activity"] = False

    def __get_prev_first_act(self, morning_trips: pd.DataFrame):
        return (
            self.activities_raw["unique_id"].isin(morning_trips["unique_id"]) & self.activities_raw["is_first_activity"]
        )

    def __neglect_overlap_morning_trips(
        self, morning_trips: pd.DataFrame, is_prev_first_acts: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.Series]:
        # Option 1 of treating overlaps: After concatenation in the end
        first_trips_end = self.activities_raw.loc[is_prev_first_acts, "timestamp_end"].copy()
        first_trips_end.index = morning_trips.index  # Adjust index for comparison

        # Filter out morning parts of overnight trip split for persons that already have morning trips in that period
        neglect_overnight = first_trips_end < morning_trips["timestamp_end"]
        morning_trips_no_overlap = morning_trips.loc[~neglect_overnight, :]

        # Filter out neglected activities from prev_first_acts accordingly
        indeces_neglect_overnight = neglect_overnight
        indeces_neglect_overnight.index = is_prev_first_acts[is_prev_first_acts].index  # Align index for filtering
        indeces_neglect_overnight = indeces_neglect_overnight[indeces_neglect_overnight]
        is_prev_first_acts[indeces_neglect_overnight.index] = False

        return morning_trips_no_overlap, is_prev_first_acts

    def __set_next_parking_timestamp_start(
        self,
        morning_trips: pd.DataFrame,
        is_overnight_trip: pd.Series,
        is_prev_first_acts: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Sets start timestamp of previously first activity (parking) to end timestamp of morning split of overnight trip.
        """
        timestamp_new = morning_trips.loc[is_overnight_trip, "timestamp_end"]
        timestamp_new.index = self.activities_raw.loc[is_prev_first_acts, "timestamp_start"].index
        self.activities_raw.loc[is_prev_first_acts, "timestamp_start"] = timestamp_new
        self.activities_raw.loc[is_prev_first_acts, "is_first_activity"] = False

        # Set next_activity_id column of overnight trips to consecutive activity
        return self.__update_next_activity_id(
            prev_first_acts=self.activities_raw.loc[is_prev_first_acts, :],
            morning_trips=morning_trips,
        )

    def __update_next_activity_id(self, prev_first_acts: pd.DataFrame, morning_trips: pd.DataFrame) -> pd.DataFrame:
        next_acts = prev_first_acts.loc[prev_first_acts["previous_activity_id"] == 0, "activity_id"]
        next_acts.index = morning_trips.index
        ret = morning_trips.copy()
        ret.loc[:, "next_activity_id"] = next_acts
        return ret

    def __add_morning_trips(self, morning_trips: pd.DataFrame):
        """
        Appends overnight morning trips.
        """
        self.activities_raw = pd.concat([self.activities_raw, morning_trips])

    def __remove_first_parking_act(self):
        """
        Removes first parking activities for persons where first activity is a trip (starting at 00:00).
        """
        first_park_acts = self.activities_raw.loc[self.activities_raw["park_id"] == 0, :]
        first_trip_acts = self.activities_raw.loc[self.activities_raw["trip_id"] == 1, :]
        first_trip_acts.index = first_park_acts.index  # Aligning trip indices
        indeces_park_timestamp = first_park_acts["timestamp_start"] == first_trip_acts["timestamp_start"]
        self.activities_raw = self.activities_raw.drop(indeces_park_timestamp[indeces_park_timestamp].index)

        # After removing first parking, set first trip to first activity
        self.activities_raw.loc[
            (self.activities_raw["unique_id"].isin(first_park_acts.loc[indeces_park_timestamp, "unique_id"]))
            & (self.activities_raw["trip_id"] == 1),
            "is_first_activity",
        ] = True

    def __merge_adjacent_trips(self):
        """
        Consolidate overnight morning trips and first trips for the edge case where morning trips of next day
        end exactly at the beginning of the first trip of the survey day. In this case, the morning split of the
        overnight trip is neglected and the beginning of the first trip is set to 00:00. In the MiD17 data set, there
        were 3 occurences of this case all with end times of the overnight trip between 00:00 and 01:00.
        """
        unique_id = self.__get_unique_ids_to_neglect()
        self.__neglect_zero_trip_id_from_activities(id_neglect=unique_id)
        self.__update_consolidated_act(id_neglect=unique_id)

    def __check_and_assert(self):
        # Calculates the neglected trip distances from overnight split trips with regular morning trips
        distance = (
            self.activities_raw["trip_distance"].sum()
            - self.activities_raw.loc[~self.activities_raw["trip_id"].isna(), "trip_distance"].sum()
        )
        all_trip_distance = self.activities_raw.loc[~self.activities_raw["trip_id"].isna(), "trip_distance"].sum()
        ratio = distance / all_trip_distance
        print(
            f"From {round(all_trip_distance, 2)} km total mileage in the dataset after filtering, {round((ratio * 100), 2)}% were cropped "
            f"because they corresponded to split-trips from overnight trips."
        )
        assert ratio < 0.01

    def __get_unique_ids_to_neglect(self) -> pd.DataFrame:
        """
        Identifies the household person IDs that should be neglected.
        """
        unique_ids_overnight = self.activities_raw.loc[self.activities_raw["trip_id"] == 0, "unique_id"]
        activities = self.activities_raw.loc[self.activities_raw["unique_id"].isin(unique_ids_overnight), :]
        activities_overnight = activities.loc[activities["trip_id"] == 0, :]
        # Next trip after morning part of overnight split
        acts_next_trip = activities.loc[activities["previous_activity_id"] == 0, :]
        return activities_overnight.loc[
            ~activities_overnight["unique_id"].isin(acts_next_trip["unique_id"]), "unique_id"
        ]

    def __neglect_zero_trip_id_from_activities(self, id_neglect: pd.Series):
        """
        Filters out the activities with the given hhpid and trip_id 0.
        """
        neglect = (self.activities_raw["unique_id"].isin(id_neglect)) & (self.activities_raw["trip_id"] == 0)
        self.activities_raw = self.activities_raw.loc[~neglect, :]

    def __update_consolidated_act(self, id_neglect: pd.Series):
        """
        Sets the start timestamp of the firstActivity of all hhpids given as argument to 00:00. Additionally
        the previous_activity_id is set to pd.NA.
        """
        indeces_consolidated_trips = (self.activities_raw["unique_id"].isin(id_neglect)) & (
            self.activities_raw["is_first_activity"]
        )
        self.activities_raw.loc[indeces_consolidated_trips, "timestamp_start"] = replace_vec(
            self.activities_raw.loc[indeces_consolidated_trips, "timestamp_start"],
            hour=0,
            minute=0,
        )
        self.activities_raw.loc[indeces_consolidated_trips, "previous_activity_id"] = pd.NA

    def __sort_activities(self):
        """
        Sorts activities according to unique_id and timestamp_start column values.
        """
        self.activities_raw = self.activities_raw.sort_values(by=["unique_id", "timestamp_start"])
