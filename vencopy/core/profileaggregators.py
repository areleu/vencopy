__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.04.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time
from pathlib import Path
import pandas as pd

from vencopy.core.diarybuilders import DiaryBuilder
from vencopy.utils.utils import create_file_name, write_out


class ProfileAggregator:
    def __init__(self, configs: dict, activities: pd.DataFrame, profiles: DiaryBuilder):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = self.user_config["global"]["dataset"]
        self.weighted = self.user_config["profileaggregators"]["weight_flow_profiles"]
        self.alpha = self.user_config["profileaggregators"]["alpha"]
        self.activities = activities
        self.profiles = profiles
        self.weights = (
            self.activities.loc[:, ["unique_id", "trip_weight"]]
            .drop_duplicates(subset=["unique_id"])
            .reset_index(drop=True)
            .set_index("unique_id")
        )
        self.drain = profiles.drain
        self.charging_power = profiles.charging_power
        self.uncontrolled_charging = profiles.uncontrolled_charging
        self.max_battery_level = profiles.max_battery_level
        self.min_battery_level = profiles.min_battery_level
        self.drain_weekly = None
        self.charging_power_weekly = None
        self.uncontrolled_charging_weekly = None
        self.max_battery_level_weekly = None
        self.min_battery_level_weekly = None
        self.aggregator = Aggregator(
            activities=self.activities,
            dataset=self.dataset,
            user_config=self.user_config,
            dev_config=self.dev_config,
            weighted=self.weighted,
            alpha=self.alpha,
        )

    def aggregate_profiles(self):
        self.drain_weekly = self.aggregator.perform_aggregation(profile=self.drain, profile_name="drain", method="flow")
        self.charging_power_weekly = self.aggregator.perform_aggregation(
            profile=self.charging_power, profile_name="charging_power", method="flow"
        )
        self.uncontrolled_charging_weekly = self.aggregator.perform_aggregation(
            profile=self.uncontrolled_charging, profile_name="uncontrolled_charge", method="flow"
        )
        self.max_battery_level_weekly = self.aggregator.perform_aggregation(
            profile=self.max_battery_level, profile_name="max_battery_level", method="state"
        )
        self.min_battery_level_weekly = self.aggregator.perform_aggregation(
            profile=self.min_battery_level, profile_name="min_battery_level", method="state"
        )
        print("Aggregation finished for all profiles.")


class Aggregator:
    def __init__(
        self, activities: pd.DataFrame, dataset: str, alpha: int, user_config: dict, dev_config: dict, weighted: bool
    ):
        self.dataset = dataset
        self.alpha = alpha
        self.activities = activities
        self.weighted = weighted
        self.user_config = user_config
        self.dev_config = dev_config

    def __basic_aggregation(self) -> pd.Series:
        if self.user_config["profileaggregators"]["aggregation_timespan"] == "daily":
            self._aggregate_daily()
        elif self.user_config["profileaggregators"]["aggregation_timespan"] == "weekly":
            self._aggregate_weekly()
            self.__compose_week_profile()
        else:
            NotImplementedError("The aggregation timespan can either be daily or weekly.")

    def _aggregate_daily(self):
        self.daily_profile = pd.DataFrame(columns=self.profile.columns, index=range(1, 2))
        cols = ["unique_id", "trip_weight"]
        self.activities_subset = (
            self.activities[cols].copy().drop_duplicates(subset=["unique_id"]).reset_index(drop=True)
        )
        self.activities_weekday = pd.merge(self.profile, self.activities_subset, on="unique_id", how="inner")
        self.activities_weekday = self.activities_weekday.set_index("unique_id")
        # Aggregate across all days, independently of day of the week
        if self.method == "flow":
            if self.weighted:
                # weekday_subset = weekday_subset.drop("trip_start_weekday", axis=1)
                # aggregate activities_weekday to one profile by multiplying by weights
                weight_sum = sum(self.activities_weekday.trip_weight)
                daily_subset_weight = self.activities_weekday.apply(
                    lambda x: x * self.activities_weekday.trip_weight.values
                )
                daily_subset_weight = daily_subset_weight.drop("trip_weight", axis=1)
                daily_subset_weight_agg = daily_subset_weight.sum() / weight_sum
                self.daily_profile = daily_subset_weight_agg
            else:
                daily_subset = self.activities_weekday.drop(columns=["trip_weight"], axis=1).reset_index(drop=True)
                self.daily_profile_agg = daily_subset.mean(axis=0)
        elif self.method == "state":
            daily_subset = self.activities_weekday.drop(columns=["trip_weight"]).reset_index(drop=True)
            daily_subset = daily_subset.convert_dtypes()
            if self.profile_name == "max_battery_level":
                self.daily_profile = daily_subset.quantile(1 - (self.alpha / 100))
            elif self.profile_name == "min_battery_level":
                self.daily_profile = daily_subset.quantile(self.alpha / 100)
            else:
                raise NotImplementedError(f"An unknown profile {self.profile_name} was selected.")

    def _aggregate_weekly(self, by_column: str = "trip_start_weekday"):
        self.weekday_profiles = pd.DataFrame(columns=self.profile.columns, index=range(1, 8))
        cols = ["unique_id", "trip_weight"] + [by_column]
        self.activities_subset = (
            self.activities[cols].copy().drop_duplicates(subset=["unique_id"]).reset_index(drop=True)
        )
        self.activities_weekday = pd.merge(self.profile, self.activities_subset, on="unique_id", how="inner")
        # self.profile.drop('unique_id', axis=1, inplace=True)
        self.activities_weekday = self.activities_weekday.set_index("unique_id")
        # Compose weekly profile from 7 separate profiles
        if self.method == "flow":
            if self.weighted:
                self.__caculate_weighted_mean_flow_profiles(by_column="trip_start_weekday")
            else:
                self.__calculate_average_flow_profiles(by_column="trip_start_weekday")
        elif self.method == "state":
            self.__aggregate_state_profiles(by_column="trip_start_weekday", alpha=self.alpha)

    def __calculate_average_flow_profiles(self, by_column: str):
        for idate in self.activities_weekday[by_column].unique():
            weekday_subset = self.activities_weekday[self.activities_weekday[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop(columns=["trip_start_weekday", "trip_weight"], axis=1)
            weekday_subset_agg = weekday_subset.mean(axis=0)
            self.weekday_profiles.iloc[idate - 1] = weekday_subset_agg

    def __caculate_weighted_mean_flow_profiles(self, by_column: str):
        for idate in self.activities_weekday[by_column].unique():
            weekday_subset = self.activities_weekday[self.activities_weekday[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop("trip_start_weekday", axis=1)
            # aggregate activities_weekday to one profile by multiplying by weights
            weight_sum = sum(weekday_subset.trip_weight)
            weekday_subset_weight = weekday_subset.apply(lambda x: x * weekday_subset.trip_weight.values)
            weekday_subset_weight = weekday_subset_weight.drop("trip_weight", axis=1)
            weekday_subset_weight_agg = weekday_subset_weight.sum() / weight_sum
            self.weekday_profiles.iloc[idate - 1] = weekday_subset_weight_agg

    def __aggregate_state_profiles(self, by_column: str, alpha: int = 10):
        """
        Selects the alpha (100 - alpha) percentile from maximum battery level
        (minimum batttery level) profile for each hour. If alpha = 10, the
        10%-biggest (10%-smallest) value is selected, all values beyond are
        disregarded as outliers.

        :param by_column: Currently trip_weekday
        :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be
            fulfilled after selection.
        :return: No return. Result is written to self.weekday_profiles with bins
            in the columns and weekday identifiers in the rows.
        """
        for idate in self.activities_weekday[by_column].unique():
            levels = self.activities_weekday.copy()
            weekday_subset = levels[levels[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop(columns=["trip_start_weekday", "trip_weight"])
            weekday_subset = weekday_subset.convert_dtypes()
            if self.profile_name == "max_battery_level":
                self.weekday_profiles.iloc[idate - 1] = weekday_subset.quantile(alpha / 100)
            elif self.profile_name == "min_battery_level":
                self.weekday_profiles.iloc[idate - 1] = weekday_subset.quantile(1 - (alpha / 100))
            else:
                raise NotImplementedError(f"An unknown profile {self.profile_name} was selected.")

    def __compose_week_profile(self):
        # input is self.weekday_profiles. Method only works if aggregation is weekly
        # check if any day of the week is not filled, copy line above in that case
        if self.weekday_profiles.isna().any(axis=1).any():
            index_empty_rows = self.weekday_profiles[self.weekday_profiles.isna().any(axis=1)].index - 1
            for empty_row in index_empty_rows:
                if empty_row == 6:
                    self.weekday_profiles.iloc[empty_row] = self.weekday_profiles.iloc[empty_row - 1]
                else:
                    self.weekday_profiles.iloc[empty_row] = self.weekday_profiles.iloc[empty_row + 1]
        self.weekday_profiles.index.name = "weekday"
        self.weekday_profiles = self.weekday_profiles.stack().unstack(0)
        self.weekly_profile = pd.concat(
            [
                self.weekday_profiles[1],
                self.weekday_profiles[2],
                self.weekday_profiles[3],
                self.weekday_profiles[4],
                self.weekday_profiles[5],
                self.weekday_profiles[6],
                self.weekday_profiles[7],
            ],
            ignore_index=True,
        )

    def __write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["aggregator_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["aggregator_output"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manual_label=self.profile_name,
                file_name_id="output_profileAggregator",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)

    def perform_aggregation(self, profile: pd.DataFrame, profile_name: str, method: str) -> pd.DataFrame:
        self.profile = profile
        self.profile_name = profile_name
        self.method = method
        print(f"Starting to aggregate {self.profile_name} to fleet level based on day of the week.")
        start_time_agg = time.time()
        self.__basic_aggregation()
        if self.user_config["global"]["write_output_to_disk"]["aggregator_output"]:
            self.__write_output()
        print(f"Aggregation finished for {self.profile_name}.")
        elapsed_time_agg = time.time() - start_time_agg
        print(f"Needed time to aggregate {self.profile_name}: {elapsed_time_agg}.")
        self.profile = None
        self.profile_name = None
        self.method = None
        return self.weekly_profile
