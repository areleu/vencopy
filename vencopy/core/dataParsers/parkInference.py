__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod

import pandas as pd

from vencopy.utils.globalFunctions import replace_vec


class ParkInference:
    def __init__(self, configs) -> None:
        self.user_config = configs["user_config"]
        self.activities = None
        self.overnight_splitter = OvernightSplitter()

    def add_parking_rows(self, trips: pd.DataFrame) -> pd.DataFrame:
        """
        Wrapper function generating park activity rows between the trip data from the original MID dataset. Some
        utility attributes are being added such as isFirstActivity, isLastActivity or the uniqueID of the next and
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
        split_overnight_trips = self.user_config["dataParsers"]["splitOvernightTrips"]
        self.__copy_rows()
        self.__add_util_attributes()
        self.__add_park_act_after_last_trip()
        self.__adjust_park_attrs()
        self._drop_redundant_cols()
        self.__remove_next_day_park_acts()
        self.__adjust_park_timestamps()
        self.__set_trip_attrs_na_for_park_acts()
        self.__add_next_and_prev_ids()
        self.__ON_split_decider(split=split_overnight_trips)  # ON = overnight
        self.__add_timedelta_col()
        self.__unique_idx()
        print(
            f'Finished activity composition with {self.trips["tripID"].fillna(0).astype(bool).sum()} trips '
            f'and {self.trips["parkID"].fillna(0).astype(bool).sum()} parking activites.'
        )
        return self.trips

    def __copy_rows(self):
        # Adding skeleton duplicate rows for parking activities
        self.trips = pd.concat([self.trips] * 2).sort_index(ignore_index=True)
        self.trips["parkID"] = self.trips["tripID"]
        self.trips.loc[range(0, len(self.trips), 2), "tripID"] = pd.NA
        self.trips.loc[range(1, len(self.trips), 2), "parkID"] = pd.NA

    def __add_util_attributes(self):
        # Adding additional attribute columns for convenience
        self.trips["uniqueID_prev"] = self.trips["uniqueID"].shift(fill_value=0)
        self.trips["isFirstActivity"] = self.trips["uniqueID_prev"] != self.trips["uniqueID"]
        self.trips["uniqueID_next"] = self.trips["uniqueID"].shift(-1, fill_value=0)
        self.trips["isLastActivity"] = self.trips["uniqueID_next"] != self.trips["uniqueID"]

    def __add_park_act_after_last_trip(self):
        # Adding park activities after last trips
        new_idx = self.trips.index[self.trips.isLastActivity]
        df_add = self.trips.loc[new_idx, :]
        df_add["tripID"] = pd.NA
        self.trips.loc[new_idx, "isLastActivity"] = False
        df_add["parkID"] = self.trips.loc[new_idx, "tripID"] + 1
        self.trips = pd.concat([self.trips, df_add]).sort_index()

    def __adjust_park_attrs(self):
        # Setting trip attribute values to zero where tripID == NaN (i.e. for all parking activities)
        self.trips.loc[
            self.trips["tripID"].isna(),
            ["tripDistance", "travelTime", "tripIsIntermodal"],
        ] = pd.NA
        self.trips["colFromIndex"] = self.trips.index
        self.trips = self.trips.sort_values(by=["colFromIndex", "tripID"])

    def _drop_redundant_cols(self):
        # Clean-up of temporary redundant columns
        self.trips.drop(
            columns=[
                "tripStartClock",
                "tripEndClock",
                "tripStartYear",
                "tripStartMonth",
                "tripStartWeek",
                "tripStartHour",
                "tripStartMinute",
                "tripEndHour",
                "tripEndMinute",
                "uniqueID_prev",
                "uniqueID_next",
                "colFromIndex",
            ],
            inplace=True,
        )

    def __remove_next_day_park_acts(self):
        # Checking for trips across day-limit and removing respective parking activities
        ON_idx = self.trips["isLastActivity"] & self.trips["tripEndNextDay"]
        ON_idx = ON_idx.loc[ON_idx]
        self.trips.loc[ON_idx.index, "isLastActivity"] = True
        self.trips = self.trips.reset_index()

        # Get rid of park activities after overnight trips
        idx_multi_day_act = self.trips["isLastActivity"] & self.trips["tripEndNextDay"] & self.trips["parkID"]
        self.trips = self.trips.loc[~idx_multi_day_act, :]

    def __adjust_park_timestamps(self):
        """Adjust the start and end timestamps of the newly added rows. This is done via range index, that is reset at
        the beginning. First and last activities have to be treated separately since their dates have to match with
        their daily activity chain.
        """

        self.trips = self.trips.reset_index()
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
        park_act = ~self.trips["parkID"].isna()
        park_act = park_act.loc[park_act]
        return park_act.iloc[1:], park_act.iloc[:-1]

    def __update_park_start(self, park_act_wo_first: pd.Series):
        """Updating park start timestamps for newly added rows"""
        set_ts = self.trips.loc[park_act_wo_first.index - 1, "timestampEnd"]
        set_ts.index = self.trips.loc[park_act_wo_first.index, "timestampStart"].index
        self.trips.loc[park_act_wo_first.index, "timestampStart"] = set_ts

    def __update_park_end(self, park_act_wo_last: pd.Series):
        """Updating park end timestamps for newly added rows"""
        set_ts = self.trips.loc[park_act_wo_last.index + 1, "timestampStart"]
        set_ts.index = self.trips.loc[park_act_wo_last.index, "timestampEnd"].index
        self.trips.loc[park_act_wo_last.index, "timestampEnd"] = set_ts

    def __update_timestamp_first_park_act(self):
        """Updating park end timestamps for last activity in new park rows"""
        idx_acts = ~(self.trips["parkID"].isna()) & (self.trips["isFirstActivity"])
        self.trips.loc[idx_acts, "timestampStart"] = replace_vec(
            self.trips.loc[idx_acts, "timestampEnd"], hour=0, minute=0
        )

    def __update_timestamp_last_park_act(self):
        """Updating park end timestamps for last activity in new park rows"""
        idx_acts = ~(self.trips["parkID"].isna()) & (self.trips["isLastActivity"])
        self.trips.loc[idx_acts, "timestampEnd"] = replace_vec(
            self.trips.loc[idx_acts, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")

    def __set_trip_attrs_na_for_park_acts(self):
        # Set tripEndNextDay to False for all park activities
        self.trips.loc[self.trips["tripID"].isna(), "tripEndNextDay"] = pd.NA

    def __add_next_and_prev_ids(self):
        self.trips.loc[~self.trips["tripID"].isna(), "actID"] = self.trips["tripID"]
        self.trips.loc[~self.trips["parkID"].isna(), "actID"] = self.trips["parkID"]
        self.trips.loc[~self.trips["isLastActivity"], "nextActID"] = self.trips.loc[:, "actID"].shift(-1)
        self.trips.loc[~self.trips["isFirstActivity"], "prevActID"] = self.trips.loc[:, "actID"].shift(1)

    def __ON_split_decider(self, split: bool):
        """Boolean function that differentiates if overnight trips should be split (split==True) or not (split==False).
        In the latter case, overnight trips identified by the variable 'tripEndNextDay' are excluded from the data set.

        Args:
            split (bool): Should trips that end on the consecutive day (not the survey day) be split in two trips in
            such a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        if split:
            self.trips = self.overnight_splitter.split_overnight_trips(trips=self.trips)
        else:
            self.__set_ON_var_false_for_last_act_trip()
            self.__neglect_ON_trips()

    def __set_ON_var_false_for_last_act_trip(self):
        """This function treats the edge case of trips being the last activity in the daily activity chain, i.e. trips
        ending exactly at 00:00. They are falsely labelled as overnight trips which is corrected here.

        """
        idx_last_act_trips = (self.trips["isLastActivity"]) & ~(self.trips["tripID"].isna())
        idx_last_trip_end_midnight = (
            idx_last_act_trips
            & (self.trips.loc[idx_last_act_trips, "timestampEnd"].dt.hour == 0)
            & (self.trips.loc[idx_last_act_trips, "timestampEnd"].dt.minute == 0)
        )
        self.trips_end_next_day_raw = self.trips["tripEndNextDay"]
        self.trips.loc[idx_last_trip_end_midnight, "tripEndNextDay"] = False

    def __neglect_ON_trips(self):
        """
        Removes all overnight trips from the activities data set based on the column 'tripEndNextDay'. Updates
        timestamp end (to 00:00) and isLastActivity for the new last parking activities. Overwrites self.trips.
        """
        # Column for lastActivity setting later
        self.trips["tripEndNextDay_next"] = self.trips["tripEndNextDay"].shift(-1, fill_value=False)

        # Get rid of overnight trips
        idx_no_ON_trip = ~(self.trips["tripEndNextDay"].fillna(False))
        self.trips = self.trips.loc[idx_no_ON_trip, :]

        # Update isLastActivity and timestampEnd variables and clean-up column
        idx_new_last_act = self.trips["tripEndNextDay_next"]
        idx_new_last_act = idx_new_last_act.fillna(False).astype(bool)
        self.trips.loc[idx_new_last_act, "isLastActivity"] = True
        self.trips.loc[idx_new_last_act, "timestampEnd"] = replace_vec(
            self.trips.loc[idx_new_last_act, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")
        self.trips = self.trips.drop(columns=["tripEndNextDay_next"])

    def __add_timedelta_col(self):
        # Add timedelta column
        self.trips["timedelta"] = self.trips["timestampEnd"] - self.trips["timestampStart"]

    def __unique_idx(self):
        self.trips.drop(columns=["level_0"], inplace=True)
        self.trips.reset_index(inplace=True)  # Due to copying and appending rows, the index has to be reset


class OvernightSplitter:
    def __init__(self):
        self.trips = None

    def split_overnight_trips(self, trips: pd.DataFrame) -> pd.DataFrame:
        """Wrapper function for treating edge case trips ending not in the 24 hours of the survey day but stretch
        to the next day. Those overnight (ON) are split up into an evening trip at the regular survey day and a
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
        self.trips = trips

        # Split overnight trips and add next day distance in the morning (tripID=0)
        is_ON_trip, ON_trips_add = self.__get_ON_acts()
        ON_trips_add_timestamp = self.__adjust_ON_timestamps(trips=ON_trips_add)
        self.__set_all_last_act_end_TS_to_zero()
        morning_trips = self.__set_ON_trip_ID_zero(trips=ON_trips_add_timestamp)
        morning_trips = self.__adjust_morning_trip_distance(overnightTrips=ON_trips_add, morning_trips=morning_trips)
        self.__adjust_evening_trip_distance(morning_trips=morning_trips, is_ON_trip=is_ON_trip)
        self.__set_first_last_acts(morning_trips=morning_trips)
        is_prev_first_acts = self.__get_prev_first_act(
            morning_trips=morning_trips
        )  # Activities that were previously firstActs
        morning_trips_no_overlap, is_prev_first_acts = self.__neglect_overlap_morning_trips(
            morning_trips=morning_trips, is_prev_first_acts=is_prev_first_acts
        )
        morningTrips_add = self.__set_next_parking_TS_start(
            morning_trips=morning_trips_no_overlap, is_ON_trip=is_ON_trip, is_prev_first_acts=is_prev_first_acts
        )
        self.__add_morning_trips(morning_trips=morningTrips_add)
        self.__remove_first_parking_act()
        self.__merge_adjacent_trips()
        # Implement DELTA mileage check of overnight morning split trip distances
        self.__check_and_assert()
        self.__dropONCol()
        self.__sortActivities()
        return self.trips

    def __get_ON_acts(self) -> tuple[pd.Series, pd.DataFrame]:
        indexOvernightActs = (
            self.trips["isLastActivity"]
            & self.trips["tripEndNextDay"]
            & ~(
                (self.trips["timestampEnd"].dt.hour == 0)
                & (self.trips["timestampEnd"].dt.minute == 0)  # assure that the overnight trip does
            )
        )  # not exactly end at 00:00
        return indexOvernightActs, self.trips.loc[indexOvernightActs, :]

    def __adjust_ON_timestamps(self, trips: pd.DataFrame) -> pd.DataFrame:
        tripsRes = trips.copy()
        tripsRes["timestampEnd"] = tripsRes.loc[:, "timestampEnd"] - pd.Timedelta(1, "d")
        tripsRes["timestampStart"] = replace_vec(tripsRes.loc[:, "timestampEnd"], hour=0, minute=0)
        return tripsRes

    def __set_all_last_act_end_TS_to_zero(self):
        # Set timestamp end of evening part of overnight trip split to 00:00
        self.trips.loc[self.trips["isLastActivity"], "timestampEnd"] = replace_vec(
            self.trips.loc[self.trips["isLastActivity"], "timestampEnd"],
            hour=0,
            minute=0,
        )

    def __set_ON_trip_ID_zero(self, trips: pd.DataFrame) -> pd.DataFrame:
        trips["tripID"] = 0
        trips["actID"] = 0
        trips["prevActID"] = pd.NA

        # Update next activity ID
        unique_ID = trips["uniqueID"]
        act_idx = self.trips["uniqueID"].isin(unique_ID) & self.trips["isFirstActivity"]
        trips["nextActID"] = self.trips.loc[act_idx, "actID"]

        # Update previous activity ID of previously first activity
        self.trips.loc[act_idx, "prevActID"] = 0
        return trips

    def __adjust_morning_trip_distance(self, overnightTrips: pd.DataFrame, morning_trips: pd.DataFrame) -> pd.DataFrame:
        # Splitting the total distance to morning and evening trip time-share dependent
        morning_trips["timedelta_total"] = overnightTrips["timestampEnd"] - overnightTrips["timestampStart"]
        morning_trips["timedelta_morning"] = morning_trips["timestampEnd"] - morning_trips["timestampStart"]
        morning_trips["timeShare_morning"] = morning_trips["timedelta_morning"] / morning_trips["timedelta_total"]
        morning_trips["timeShare_evening"] = (
            morning_trips["timedelta_total"] - morning_trips["timedelta_morning"]
        ) / morning_trips["timedelta_total"]
        morning_trips["totalTripDistance"] = morning_trips["tripDistance"]
        morning_trips["tripDistance"] = morning_trips["timeShare_morning"] * morning_trips["totalTripDistance"]
        return morning_trips

    def __adjust_evening_trip_distance(self, morning_trips: pd.DataFrame, is_ON_trip: pd.Series):
        self.trips.loc[is_ON_trip, "tripDistance"] = (
            morning_trips["timeShare_evening"] * morning_trips["totalTripDistance"]
        )

    def __set_first_last_acts(self, morning_trips: pd.DataFrame):
        # Setting first and last activities
        morning_trips["isFirstActivity"] = True
        morning_trips["isLastActivity"] = False

    def __get_prev_first_act(self, morning_trips: pd.DataFrame):
        return self.trips["uniqueID"].isin(morning_trips["uniqueID"]) & self.trips["isFirstActivity"]

    def __neglect_overlap_morning_trips(
        self, morning_trips: pd.DataFrame, is_prev_first_acts: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.Series]:
        # Option 1 of treating overlaps: After concatenation in the end
        first_trips_end = self.trips.loc[is_prev_first_acts, "timestampEnd"].copy()
        first_trips_end.index = morning_trips.index  # Adjust index for comparison

        # Filter out morning parts of overnight trip split for persons that already have morning trips in that period
        neglect_ON = first_trips_end < morning_trips["timestampEnd"]
        morning_trips_no_overlap = morning_trips.loc[~neglect_ON, :]

        # Filter out neglected activities from prev_first_acts accordingly
        neglect_ON_idx = neglect_ON
        neglect_ON_idx.index = is_prev_first_acts[is_prev_first_acts].index  # Align index for filtering
        neglect_ON_idx = neglect_ON_idx[neglect_ON_idx]
        is_prev_first_acts[neglect_ON_idx.index] = False

        return morning_trips_no_overlap, is_prev_first_acts

    def __set_next_parking_TS_start(
        self,
        morning_trips: pd.DataFrame,
        is_ON_trip: pd.Series,
        is_prev_first_acts: pd.DataFrame,
    ) -> pd.DataFrame:
        # Setting start timestamp of previously first activity (parking) to end timestamp of morning split of ON trip
        ts_new = morning_trips.loc[is_ON_trip, "timestampEnd"]
        ts_new.index = self.trips.loc[is_prev_first_acts, "timestampStart"].index
        self.trips.loc[is_prev_first_acts, "timestampStart"] = ts_new
        self.trips.loc[is_prev_first_acts, "isFirstActivity"] = False

        # Set nextActID column of ON trips to consecutive activity
        return self.__update_next_act_ID(
            prev_first_acts=self.trips.loc[is_prev_first_acts, :],
            morning_trips=morning_trips,
        )

    def __update_next_act_ID(self, prev_first_acts: pd.DataFrame, morning_trips: pd.DataFrame) -> pd.DataFrame:
        next_acts = prev_first_acts.loc[prev_first_acts["prevActID"] == 0, "actID"]
        next_acts.index = morning_trips.index
        ret = morning_trips.copy()
        ret.loc[:, "nextActID"] = next_acts
        return ret

    def __add_morning_trips(self, morning_trips: pd.DataFrame):
        # Appending overnight morning trips
        self.trips = pd.concat([self.trips, morning_trips])

    def __remove_first_parking_act(self):
        # Removing first parking activities for persons where first activity is a trip (starting at 00:00)
        first_park_acts = self.trips.loc[self.trips["parkID"] == 1, :]
        first_trip_acts = self.trips.loc[self.trips["tripID"] == 1, :]
        first_trip_acts.index = first_park_acts.index  # Aligning trip indices
        idx_park_TS = first_park_acts["timestampStart"] == first_trip_acts["timestampStart"]
        self.trips = self.trips.drop(idx_park_TS[idx_park_TS].index)

        # After removing first parking, set first trip to first activity
        self.trips.loc[
            (self.trips["uniqueID"].isin(first_park_acts.loc[idx_park_TS, "uniqueID"])) & (self.trips["tripID"] == 1),
            "isFirstActivity",
        ] = True

    def __merge_adjacent_trips(self):
        """Consolidate overnight morning trips and first trips for the edge case where morning trips of next day
        end exactly at the beginning of the first trip of the survey day. In this case, the morning split of the
        overnight trip is neglected and the beginning of the first trip is set to 00:00. In the MiD17 data set, there
        were 3 occurences of this case all with end times of the overnight trip between 00:00 and 01:00.

        """
        unique_ID = self.__get_unique_IDs_to_neglect()
        self.__neglect_zero_trip_ID_from_acts(id_neglect=unique_ID)
        self.__update_consolidated_act(id_neglect=unique_ID)

    def __check_and_assert(self):
        # Calculates the neglected trip distances from overnight split trips with regular morning trips
        distance = self.trips["tripDistance"].sum() - self.trips.loc[~self.trips["tripID"].isna(), "tripDistance"].sum()
        all_trip_distance = self.trips.loc[~self.trips["tripID"].isna(), "tripDistance"].sum()
        ratio = distance / all_trip_distance
        print(
            f"From {all_trip_distance} km total mileage in the dataset after filtering, {ratio * 100}% were cropped "
            f"because they corresponded to split-trips from overnight trips."
        )
        assert ratio < 0.01

    def __get_unique_IDs_to_neglect(self) -> pd.DataFrame:
        """
        Identifies the household person IDs that should be neglected.
        """
        unique_IDs_ON = self.trips.loc[self.trips["tripID"] == 0, "uniqueID"]
        acts = self.trips.loc[self.trips["uniqueID"].isin(unique_IDs_ON), :]
        acts_ON = acts.loc[acts["tripID"] == 0, :]
        # Next trip after morning part of overnight split
        acts_next_trip = acts.loc[acts["prevActID"] == 0, :]
        return acts_ON.loc[~acts_ON["uniqueID"].isin(acts_next_trip["uniqueID"]), "uniqueID"]

    def __neglect_zero_trip_ID_from_acts(self, id_neglect: pd.Series):
        """
        This method filters out the activities with the given hhpid and tripID 0.
        """
        neglect = (self.trips["uniqueID"].isin(id_neglect)) & (self.trips["tripID"] == 0)
        self.trips = self.trips.loc[~neglect, :]

    def __update_consolidated_act(self, id_neglect: pd.Series):
        """
        This method sets the start timestamp of the firstActivity of all hhpids given as argument to 00:00. Additionally
        the prevActID is set to pd.NA
        """
        idx_consolidated_trips = (self.trips["uniqueID"].isin(id_neglect)) & (self.trips["isFirstActivity"])
        self.trips.loc[idx_consolidated_trips, "timestampStart"] = replace_vec(
            self.trips.loc[idx_consolidated_trips, "timestampStart"],
            hour=0,
            minute=0,
        )
        self.trips.loc[idx_consolidated_trips, "prevActID"] = pd.NA

    def __dropONCol(self):
        self.trips = self.trips.drop(columns=["tripEndNextDay"])

    def __sortActivities(self):
        self.trips = self.trips.sort_values(by=["uniqueID", "timestampStart"])
