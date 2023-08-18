__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from vencopy.utils.globalFunctions import create_file_name, write_out


class DiaryBuilder:
    def __init__(self, configs: dict, activities: pd.DataFrame, is_week_diary: bool = False):
        self.dev_config = configs["dev_config"]
        self.user_config = configs["user_config"]
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.activities = activities
        self.delta_time = configs["user_config"]["diaryBuilders"]["TimeDelta"]
        self.is_week_diary = is_week_diary
        self.__update_activities()
        self.drain = None
        self.charging_power = None
        self.uncontrolled_charging = None
        self.max_battery_level = None
        self.min_battery_level = None
        self.distributor = TimeDiscretiser(
            dataset=self.dataset,
            dev_config=self.dev_config,
            user_config=self.user_config,
            activities=self.activities,
            dt=self.delta_time,
            is_week=is_week_diary,
        )

    def __update_activities(self):
        """
        Updates timestamps and removes activities whose length equals zero to avoid inconsistencies in profiles
        which are separatly discretised (interdependence at single vehicle level of drain, charging power etc i.e.
        no charging available when driving).
        """
        self.__correct_timestamps()
        self.__removes_zero_length_activities()

    def __correct_timestamps(self) -> pd.DataFrame:
        """
        Rounds timestamps to predifined resolution.
        """
        self.activities["timestampStartCorrected"] = self.activities["timestamp_start"].dt.round(f"{self.delta_time}min")
        self.activities["timestampEndCorrected"] = self.activities["timestamp_end"].dt.round(f"{self.delta_time}min")
        self.activities["activityDuration"] = (
            self.activities["timestampEndCorrected"] - self.activities["timestampStartCorrected"]
        )

    def __removes_zero_length_activities(self):
        """
        Drops line when activity duration is zero, which causes inconsistencies in diaryBuilder (e.g. division by zero in number_bins calculation).
        """
        start_length = len(self.activities)
        self.activities = self.activities.drop(
            self.activities[self.activities.activityDuration == pd.Timedelta(0)].index.to_list()
        )
        end_length = len(self.activities)
        print(
            f"{start_length - end_length} activities dropped from {start_length} total activities because activity length equals zero."
        )

    def create_diaries(self):
        start_time = time.time()
        self.drain = self.distributor.discretise(profile=self.drain, profile_name="drain", method="distribute")
        self.charging_power = self.distributor.discretise(
            profile=self.charging_power, profile_name="availablePower", method="select"
        )
        self.uncontrolled_charging = self.distributor.discretise(
            profile=self.uncontrolled_charging, profile_name="uncontrolled_charging", method="dynamic"
        )
        self.max_battery_level = self.distributor.discretise(
            profile=self.max_battery_level, profile_name="maxBatteryLevelStart", method="dynamic"
        )
        self.min_battery_level = self.distributor.discretise(
            profile=self.min_battery_level, profile_name="minBatteryLevelEnd", method="dynamic"
        )
        needed_time = time.time() - start_time
        print(f"Needed time to discretise all columns: {needed_time}.")

    def calculate_uncontrolled_charging(self, maxBatLev: pd.DataFrame) -> pd.DataFrame:
        uncCharge = maxBatLev.copy()
        for cName, c in uncCharge.items():
            if cName > 0:
                tempCol = maxBatLev[cName] - maxBatLev[cName - 1]
                uncCharge[cName] = tempCol.where(tempCol >= 0, other=0)
            else:
                uncCharge[cName] = 0
        return uncCharge


class TimeDiscretiser:
    def __init__(
        self,
        activities: pd.DataFrame,
        dt: int,
        dataset: str,
        user_config: dict,
        dev_config: dict,
        is_week: bool = False,
    ):
        """
        Class for discretisation of activities to fixed temporal resolution

        Activities is a pandas Series with a unique ID in the index, ts is a pandas dataframe with two
        columns: timestamp_start and timestamp_end, dt is a pandas TimeDelta object
        specifying the fixed resolution that the discretisation should output. Method
        specifies how the discretisation should be carried out. 'Distribute' assumes
        act provides a divisible variable (energy, distance etc.) and distributes this
        depending on the time share of the activity within the respective time interval.
        'Select' assumes an undivisible variable such as power is given and selects
        the values for the given timestamps. For now: If start or end timestamp of an
        activity exactly hits the middle of a time interval (dt/2), the value is allocated
        if its ending but not if its starting (value set to 0). For dt=30 min, a parking
        activity ending at 9:15 with a charging availability of 11 kW, 11 kW will be assigned
        to the last slot (9:00-9:30) whereas if it had started at 7:45, the slot (7:30-8:00)
        is set to 0 kW.
        The quantum is the shortest possible time interval for the discretiser, hard
        coded in the init and given as a pandas.TimeDelta. Thus if 1 minute is selected
        discretisation down to resolutions of seconds are not possible.

        Args:
            activities (pd.dataFrame): _description_
            dt (pd.TimeDelta): _description_
        """
        self.activities = activities
        self.dataset = dataset
        self.data_to_discretise = None
        self.user_config = user_config
        self.dev_config = dev_config
        self.quantum = pd.Timedelta(value=1, unit="min")
        self.dt = dt  # e.g. 15 min
        self.is_week = is_week
        self.number_time_slots = int(self.__number_slots_per_interval(interval=pd.Timedelta(value=self.dt, unit="min")))
        if is_week:
            self.time_delta = pd.timedelta_range(start="00:00:00", end="168:00:00", freq=f"{self.dt}T")
            self.weekdays = self.activities["weekday_string"].unique()
        else:  # is Day
            self.time_delta = pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.dt}T")
        self.time_index = list(self.time_delta)
        self.discrete_data = None

    def __number_slots_per_interval(self, interval: pd.Timedelta) -> int:
        """
        Check if interval is an integer multiple of quantum.
        The minimum resolution is 1 min, case for resolution below 1 min.
        Then check if an integer number of intervals fits into one day (15 min equals 96 intervals)
        """
        if interval.seconds / 60 < self.quantum.seconds / 60:
            raise (
                ValueError(
                    f"The specified resolution is not a multiple of {self.quantum} minute, "
                    f"which is the minmum possible resolution"
                )
            )
        quot = interval.seconds / 3600 / 24
        quot_day = pd.Timedelta(value=24, unit="h") / interval
        if (1 / quot) % int(1 / quot) == 0:  # or (quot % int(1) == 0):
            return quot_day
        else:
            raise (
                ValueError(
                    f"The specified resolution does not fit into a day."
                    f"There cannot be {quot_day} finite intervals in a day"
                )
            )

    def __dataset_cleanup(self):
        self.__remove_columns()
        self.__correct_values()
        self.__correct_timestamps()

    def __remove_columns(self) -> pd.DataFrame:
        """
        Removes additional columns not used in the TimeDiscretiser class.
        Only keeps timestamp start and end, unique ID, and the column to discretise.
        """
        necessary_columns = [
            "trip_id",
            "timestamp_start",
            "timestamp_end",
            "unique_id",
            "park_id",
            "is_first_activity",
            "is_last_activity",
            "timedelta",
            "activity_id",
            "next_activity_id",
            "prevActID",
        ] + [self.column_to_discretise]
        if self.is_week:
            necessary_columns = necessary_columns + ["weekday_string"]
        if self.column_to_discretise == "uncontrolled_charging":
            necessary_columns = necessary_columns + ["availablePower", "timestampEndUC"]
        self.data_to_discretise = self.activities[necessary_columns].copy()

    def __correct_values(self) -> pd.DataFrame:
        """
        Depending on the columns to discretise correct some values.
        - drain profile: pads NaN with 0s
        - uncontrolled_charging profile: instead of removing rows with trip_id, assign 0 to rows with trip_id
        - residualNeed profile: pads NaN with 0s
        """
        if self.column_to_discretise == "drain":
            self.data_to_discretise["drain"] = self.data_to_discretise["drain"].fillna(0)
        elif self.column_to_discretise == "uncontrolled_charging":
            self.data_to_discretise["uncontrolled_charging"] = self.data_to_discretise["uncontrolled_charging"].fillna(0)
        elif self.column_to_discretise == "residualNeed":
            self.data_to_discretise["residualNeed"] = self.data_to_discretise["residualNeed"].fillna(0)

    def __correct_timestamps(self) -> pd.DataFrame:
        """
        Rounds timestamps to predifined resolution.
        """
        self.data_to_discretise["timestampStartCorrected"] = self.data_to_discretise["timestamp_start"].dt.round(
            f"{self.dt}min"
        )
        self.data_to_discretise["timestampEndCorrected"] = self.data_to_discretise["timestamp_end"].dt.round(f"{self.dt}min")

    def __create_discretised_structure_week(self):
        """
        Method for future release working with sampled weeks.

        Create an empty dataframe with columns each representing one timedelta (e.g. one 15-min slot). Scope can
        currently be either day (nCol = 24*60 / dt) or week - determined be self.is_week (nCol= 7 * 24 * 60 / dt).
        self.time_index is set on instantiation.
        """
        number_hours = len(list(self.time_index)) - 1
        hours_per_day = int(number_hours / len(self.weekdays))
        hours = range(hours_per_day)
        self.discrete_data = pd.DataFrame(
            index=self.data_to_discretise.unique_id.unique(),
            columns=pd.MultiIndex.from_product([self.weekdays, hours]),
        )

    def __identify_bin_shares(self):
        """
        Calculates value share to be assigned to bins and identifies the bins.
        Includes a wrapper for the 'distribute', 'select' and 'dynamic' method.
        """
        self.__calculate_number_bins()
        self.__identify_bins()
        if self.method == "distribute":
            self.__value_distribute()
        elif self.method == "select":
            self.__value_select()
        elif self.method == "dynamic":
            if self.column_to_discretise in ("maxBatteryLevelStart", "minBatteryLevelEnd"):
                self.__value_non_linear_level()
            elif self.column_to_discretise == "uncontrolled_charging":
                self.__value_non_linear_charge()
        else:
            raise (
                ValueError(
                    f'Specified method {self.method} is not implemented please specify "distribute" or "select".'
                )
            )

    def __calculate_number_bins(self):
        """
        Updates the activity duration based on the rounded timstamps.
        Calculates the multiple of dt of the activity duration and stores it to column number_bins. E.g. a 2h-activity
        with a dt of 15 mins would have a 8 in the column.
        """
        self.data_to_discretise["activityDuration"] = (
            self.data_to_discretise["timestampEndCorrected"] - self.data_to_discretise["timestampStartCorrected"]
        )
        self.__removes_zero_length_activities()
        self.data_to_discretise["number_bins"] = self.data_to_discretise["activityDuration"] / (
            pd.Timedelta(value=self.dt, unit="min")
        )
        if not self.data_to_discretise["number_bins"].apply(float.is_integer).all():
            raise ValueError("Not all bin counts are integers.")
        self.__drop_if_number_bins_length_is_zero()
        self.data_to_discretise["number_bins"] = self.data_to_discretise["number_bins"].astype(int)

    def __drop_if_number_bins_length_is_zero(self):
        """
        Drops line when number_bins is zero, which cause division by zero in number_bins calculation.
        """
        start_length = len(self.data_to_discretise)
        self.data_to_discretise.drop(self.data_to_discretise[self.data_to_discretise.number_bins == 0].index)
        end_length = len(self.data_to_discretise)
        dropped_profiles = start_length - end_length
        if dropped_profiles != 0:
            raise ValueError(f"{dropped_profiles} activities dropped because bin lenght equals zero.")

    def __value_distribute(self):
        """
        Calculates the profile value for each bin for the 'distribute' method.
        """
        if self.data_to_discretise["number_bins"].any() == 0:
            raise ArithmeticError(
                "The total number of bins is zero for one activity, which caused a division by zero."
                "This should not happen because events with length zero should have been dropped."
            )
        self.data_to_discretise["valPerBin"] = (
            self.data_to_discretise[self.column_to_discretise] / self.data_to_discretise["number_bins"]
        )

    def __value_select(self):
        """
        Calculates the profile value for each bin for the 'select' method.
        """
        self.data_to_discretise["valPerBin"] = self.data_to_discretise[self.column_to_discretise]

    def __value_non_linear_level(self):
        """
        Calculates the bin values dynamically (e.g. for the SoC). It returns a
        non-linearly increasing list of values capped to upper and lower battery
        capacity limitations. The list of values is alloacted to bins in the
        function __allocate() in the same way as for value-per-bins. Operates
        directly on class attributes thus neither input nor return attributes.
        """
        self.__delta_battery_level_driving(data=self.data_to_discretise, column=self.column_to_discretise)
        self.__delta_battery_level_charging(data=self.data_to_discretise, column=self.column_to_discretise)

    def __delta_battery_level_driving(self, data: pd.DataFrame, column: str):
        """Calculates decreasing battery level values for driving activities for
        both cases, minimum and maximum battery level. The cases have to be
        differentiated because the max case runs chronologically from morning to
        evening while the min case runs anti-chronologically from end-of-day to
        beginning. Thus, in the latter case, drain has to be added to the
        battery level.
        The function __increase_level_per_bin() is applied to the whole data set with
        the respective start battery levels (socStart), battery level increases
        (socAddPerBin) and number_bins for each activity respectively in a vectorized
        manner.
        The function adds a column 'valPerBin' to data directly, thus it doesn't
        return anything.

        Args:
            data (pd.DataFrame): Activity data with activities in rows and at least
            the columns column, 'drainPerBin', 'valPerBin', 'park_id' and
            'number_bins'.
            column (str): The column to descritize. Currently only
            maxBatteryLevelStart and minBatteryLevelStart are implemented.
        """
        if column == "maxBatteryLevelStart":
            data["drainPerBin"] = (self.activities.drain / data.number_bins) * -1
            data["valPerBin"] = data.loc[data["park_id"].isna(), :].apply(
                lambda x: self.__increase_level_per_bin(
                    socStart=x[column], socAddPerBin=x["drainPerBin"], number_bins=x["number_bins"]
                ),
                axis=1,
            )
        elif column == "minBatteryLevelEnd":
            data["drainPerBin"] = self.activities.drain / data.number_bins
            data["valPerBin"] = data.loc[data["park_id"].isna(), :].apply(
                lambda x: self.__increase_level_per_bin(
                    socStart=x[column],
                    socAddPerBin=x["drainPerBin"],
                    number_bins=x["number_bins"],
                ),
                axis=1,
            )

    def __delta_battery_level_charging(self, data: pd.DataFrame, column: str):
        """Calculates increasing battery level values for park / charging
        activities for both cases, minimum and maximum battery level. The cases
        have to be differentiated because the max case runs chronologically from
        morning to evening while the min case runs anti-chronologically from
        evening to morning. Thus, in the latter case, charge has to be
        subtracted from the battery level. Charging volumes per bin are
        calculated from the 'availablePower' column in data.
        The function __increase_level_per_bin() is applied to the whole data set with
        the respective start battery levels (socStart), battery level increases
        (socAddPerBin) and number_bins for each activity respectively in a vectorized
        manner. Then, battery capacity limitations are enforced applying the
        function __enforce_battery_limit().
        The function adds a column 'valPerBin' to data directly, thus it doesn't
        return anything.

        Args:
            data (pd.DataFrame): DataFrame with activities in rows and at least
            the columns column, 'availablePower', 'trip_id' and
            'number_bins'.
            column (str): The column to descritize. Currently only
            maxBatteryLevelStart and minBatteryLevelStart are implemented.
        """
        if column == "maxBatteryLevelStart":
            data["chargePerBin"] = self.activities.availablePower * self.dt / 60
            data.loc[data["trip_id"].isna(), "valPerBin"] = data.loc[data["trip_id"].isna(), :].apply(
                lambda x: self.__increase_level_per_bin(
                    socStart=x[column], socAddPerBin=x["chargePerBin"], number_bins=x["number_bins"]
                ),
                axis=1,
            )
            data.loc[data["trip_id"].isna(), "valPerBin"] = data.loc[data["trip_id"].isna(), "valPerBin"].apply(
                self.__enforce_battery_limit,
                how="upper",
                lim=self.user_config["flexEstimators"]["Battery_capacity"]
                * self.user_config["flexEstimators"]["Maximum_SOC"],
            )
        elif column == "minBatteryLevelEnd":
            data["chargePerBin"] = self.activities.availablePower * self.dt / 60 * -1
            data.loc[data["trip_id"].isna(), "valPerBin"] = data.loc[data["trip_id"].isna(), :].apply(
                lambda x: self.__increase_level_per_bin(
                    socStart=x[column], socAddPerBin=x["chargePerBin"], number_bins=x["number_bins"]
                ),
                axis=1,
            )
            data.loc[data["trip_id"].isna(), "valPerBin"] = data.loc[data["trip_id"].isna(), "valPerBin"].apply(
                self.__enforce_battery_limit,
                how="lower",
                lim=self.user_config["flexEstimators"]["Battery_capacity"]
                * self.user_config["flexEstimators"]["Minimum_SOC"],
            )

    def __increase_level_per_bin(self, socStart: float, socAddPerBin: float, number_bins: int) -> list:
        """Returns a list of battery level values with length number_bins starting
        with socStart with added value of socAddPerBin.

        Args:
            socStart (float): Starting SOC
            socAddPerBin (float): Consecutive (constant) additions to the start
            SOC
            number_bins (int): Number of discretized bins (one per timeslot)

        Returns:
            list: List of number_bins increasing battery level values
        """
        tmp = socStart
        lst = [tmp]
        for _ in range(number_bins - 1):
            tmp += socAddPerBin
            lst.append(tmp)
        return lst

    def __enforce_battery_limit(self, delta_battery: list, how: str, lim: float) -> list:
        """Lower-level function that caps a list of values at lower or upper
        (determined by how) limits given by limit. Thus [0, 40, 60] with
        how=upper and lim=50 would return [0, 40, 50].

        Args:
            delta_battery (list): List of float values of arbitrary length.
            how (str): Must be either 'upper' or 'lower'.
            lim (float): Number of threshold to which to limit the values in the
            list.

        Returns:
            list: Returns a list of same length with values limited to lim.
        """
        if how == "lower":
            return [max(i, lim) for i in delta_battery]
        elif how == "upper":
            return [min(i, lim) for i in delta_battery]

    def __value_non_linear_charge(self):
        self.__uncontrolled_charging_parking()
        self.__uncontrolled_charging_driving()

    def __uncontrolled_charging_parking(self):
        self.data_to_discretise["timestampEndUC"] = pd.to_datetime(self.data_to_discretise["timestampEndUC"])
        self.data_to_discretise["timedeltaUC"] = (
            self.data_to_discretise["timestampEndUC"] - self.data_to_discretise["timestamp_start"]
        )
        self.data_to_discretise["nFullBinsUC"] = (
            self.data_to_discretise.loc[self.data_to_discretise["trip_id"].isna(), "timedeltaUC"].dt.total_seconds()
            / 60
            / self.dt
        ).astype(int)
        self.data_to_discretise["valPerBin"] = self.data_to_discretise.loc[self.data_to_discretise["trip_id"].isna(), :].apply(
            lambda x: self.__charge_rate_per_bin(
                charging_rate=x["availablePower"], charged_volume=x["uncontrolled_charging"], number_bins=x["number_bins"]
            ),
            axis=1,
        )

    def __uncontrolled_charging_driving(self):
        self.data_to_discretise.loc[self.data_to_discretise["park_id"].isna(), "valPerBin"] = 0

    def __charge_rate_per_bin(self, charging_rate: float, charged_volume: float, number_bins: int) -> list:
        if charging_rate == 0:
            return [0] * number_bins
        charging_rates_per_bin = [charging_rate] * number_bins
        volumes_per_bin = [r * self.dt / 60 for r in charging_rates_per_bin]
        cEnergy = np.cumsum(volumes_per_bin)
        indeces_overshoot = [idx for idx, en in enumerate(cEnergy) if en > charged_volume]

        # Incomplete bin treatment
        if indeces_overshoot:
            bin_overshoot = indeces_overshoot.pop(0)
        # uncontrolled charging never completed during activity. This occurs when discretized activity is shorter than
        # original due to discr. e.g. unique_id == 10040082, park_id==5 starts at 16:10 and ends at 17:00, with dt=15 min
        # it has 3 bins reducing the discretized duration to 45 minutes instead of 50 minutes.
        elif cEnergy[0] < charged_volume:
            return volumes_per_bin
        else:  # uncontrolled charging completed in first bin
            return [round(charged_volume, 3)]

        if bin_overshoot == 0:
            valLastCBin = round(charged_volume, 3)
        else:
            valLastCBin = round((charged_volume - cEnergy[bin_overshoot - 1]), 3)

        return volumes_per_bin[:bin_overshoot] + [valLastCBin] + [0] * (len(indeces_overshoot))

    def __identify_bins(self):
        """
        Wrapper which identifies the first and the last bin.
        """
        self.__identify_first_bin()
        self.__identify_last_bin()

    def __identify_first_bin(self):
        """
        Identifies every first bin for each activity (trip or parking).
        """
        self.data_to_discretise["timestampStartCorrected"] = self.data_to_discretise["timestampStartCorrected"].apply(
            lambda x: pd.to_datetime(str(x))
        )
        dayStart = self.data_to_discretise["timestampStartCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.data_to_discretise["dailyTimeDeltaStart"] = self.data_to_discretise["timestampStartCorrected"] - dayStart
        self.data_to_discretise["startTimeFromMidnightSeconds"] = self.data_to_discretise["dailyTimeDeltaStart"].apply(
            lambda x: x.seconds
        )
        bins = pd.DataFrame({"binTimestamp": self.time_delta})
        bins.drop(bins.tail(1).index, inplace=True)  # remove last element, which is zero
        self.bin_from_midnight_seconds = bins["binTimestamp"].apply(lambda x: x.seconds)
        self.bin_from_midnight_seconds = self.bin_from_midnight_seconds + (self.dt * 60)
        self.data_to_discretise["firstBin"] = (
            self.data_to_discretise["startTimeFromMidnightSeconds"].apply(
                lambda x: np.argmax(x < self.bin_from_midnight_seconds)
            )
        ).astype(int)
        if self.data_to_discretise["firstBin"].any() > self.number_time_slots:
            raise ArithmeticError("One of first bin values is bigger than total number of bins.")
        if self.data_to_discretise["firstBin"].unique().any() < 0:
            raise ArithmeticError("One of first bin values is smaller than 0.")
        if self.data_to_discretise["firstBin"].isna().any():
            raise ArithmeticError("One of first bin values is NaN.")

    def __identify_last_bin(self):
        """
        Identifies every last bin for each activity (trip or parking).
        """
        dayEnd = self.data_to_discretise["timestampEndCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.data_to_discretise["dailyTimeDeltaEnd"] = self.data_to_discretise["timestampEndCorrected"] - dayEnd
        self.data_to_discretise["lastBin"] = (
            self.data_to_discretise["firstBin"] + self.data_to_discretise["number_bins"] - 1
        ).astype(int)
        if self.data_to_discretise["lastBin"].any() > self.number_time_slots:
            raise ArithmeticError("One of first bin values is bigger than total number of bins.")
        if self.data_to_discretise["lastBin"].unique().any() < 0:
            raise ArithmeticError("One of first bin values is smaller than 0.")
        if self.data_to_discretise["lastBin"].isna().any():
            raise ArithmeticError("One of first bin values is NaN.")

    def __allocate_bin_shares(self):  # sourcery skip: assign-if-exp
        """
        Wrapper which identifies shared bins and allocates them to a discrestised structure.
        """
        # self._overlappingActivities()
        self.discrete_data = self.__allocate_week() if self.is_week else self.__allocate()
        self.__check_bin_values()

    def __check_bin_values(self):
        """
        Verifies that all bins get a value assigned, otherwise raise an error.
        """
        if self.discrete_data.isna().any().any():
            raise ValueError("There are NaN in the dataset.")

    def __removes_zero_length_activities(self):
        """
        Implements a strategy for overlapping bins if time resolution high enough so that the event becomes negligible,
        i.e. drops events with no length (timestampStartCorrected = timestampEndCorrected or activityDuration = 0),
        which cause division by zero in number_bins calculation.
        """
        start_length = len(self.data_to_discretise)
        indeces_no_length_activities = self.data_to_discretise[
            self.data_to_discretise.activityDuration == pd.Timedelta(0)
        ].index.to_list()
        self.ids_with_no_length_activities = self.data_to_discretise.loc[indeces_no_length_activities]["unique_id"].unique()
        self.data_to_discretise = self.data_to_discretise.drop(indeces_no_length_activities)
        end_length = len(self.data_to_discretise)
        dropped_activities = start_length - end_length
        if dropped_activities != 0:
            raise ValueError(
                f"{dropped_activities} zero-length activities dropped from {len(self.ids_with_no_length_activities)} IDs."
            )
        self.__remove_activities_with_zero_value()

    def __remove_activities_with_zero_value(self):
        start_length = len(self.data_to_discretise)
        subsetNoLengthActivitiesIDsOnly = self.data_to_discretise.loc[
            self.data_to_discretise.unique_id.isin(self.ids_with_no_length_activities)
        ]
        subsetNoLengthActivitiesIDsOnly = subsetNoLengthActivitiesIDsOnly.set_index("unique_id", drop=False)
        subsetNoLengthActivitiesIDsOnly.index.names = ["uniqueIDindex"]
        IDsWithSumZero = subsetNoLengthActivitiesIDsOnly.groupby(["unique_id"])[self.column_to_discretise].sum()
        IDsToDrop = IDsWithSumZero[IDsWithSumZero == 0].index
        self.data_to_discretise = self.data_to_discretise.loc[~self.data_to_discretise.unique_id.isin(IDsToDrop)]
        end_length = len(self.data_to_discretise)
        dropped_activities = start_length - end_length
        if dropped_activities != 0:
            raise ValueError(
                f"Additional {dropped_activities} activities dropped as the sum of all {self.column_to_discretise} activities for the specific ID was zero."
            )

    def __allocate_week(self):
        """
        Wrapper method for allocating respective values per bin to days within a week. Expects that the activities
        are formatted in a way that unique_id represents a unique week ID. The function then loops over the 7 weekdays
        and calls __allocate for each day a total of 7 times.
        """
        raise NotImplementedError("The method has not been implemneted yet.")

    def __allocate(self) -> pd.DataFrame:
        """
        Loops over every activity (row) and allocates the respective value per bin (valPerBin) to each column
        specified in the columns firstBin and lastBin.
        Args:
            weekday (str, optional): _description_. Defaults to None.
        Returns:
            pd.DataFrame: Discretized data set with temporal discretizations in the columns.
        """
        trips = self.data_to_discretise.copy()
        trips = trips[["unique_id", "firstBin", "lastBin", "valPerBin"]]
        trips["unique_id"] = trips["unique_id"].astype(int)
        return trips.groupby(by="unique_id").apply(self.assign_bins)

    def assign_bins(self, acts: pd.DataFrame) -> pd.Series:
        """
        Assigns values for every unique_id based on first and last bin.
        """
        s = pd.Series(index=range(self.number_time_slots), dtype=float)
        for _, itrip in acts.iterrows():
            start = itrip["firstBin"]
            end = itrip["lastBin"]
            value = itrip["valPerBin"]
            if self.column_to_discretise == "minBatteryLevelEnd":
                s.loc[start:end] = value[::-1]
            else:
                s.loc[start:end] = value
        return s

    def __write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["diaryOutput"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["diaryOutput"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manual_label=self.column_to_discretise,
                file_name_id="outputDiaryBuilder",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)

    def discretise(self, profile, profile_name: str, method: str) -> pd.DataFrame:
        self.column_to_discretise: Optional[str] = profile_name
        self.data_to_discretise = profile
        self.method = method
        print(f"Starting to discretise {self.column_to_discretise}.")
        start_time_diaryBuilder = time.time()
        self.__dataset_cleanup()
        self.__identify_bin_shares()
        self.__allocate_bin_shares()
        if self.user_config["global"]["write_output_to_disk"]["diaryOutput"]:
            self.__write_output()
        print(f"Discretisation finished for {self.column_to_discretise}.")
        elapsed_time_diaryBuilder = time.time() - start_time_diaryBuilder
        print(f"Needed time to discretise {self.column_to_discretise}: {elapsed_time_diaryBuilder}.")
        self.column_to_discretise = None
        return self.discrete_data
