__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "29.07.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time
from pathlib import Path
import pandas as pd

from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.utils.globalFunctions import create_file_name, write_out


class ProfileAggregator:
    def __init__(self, configs: dict, activities: pd.DataFrame, profiles: DiaryBuilder):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = self.user_config["global"]["dataset"]
        self.weighted = self.user_config["profileAggregators"]["weightFlowProfiles"]
        self.alpha = self.user_config["profileAggregators"]["alpha"]
        self.activities = activities
        self.profiles = profiles
        self.weights = (
            self.activities.loc[:, ["uniqueID", "tripWeight"]]
            .drop_duplicates(subset=["uniqueID"])
            .reset_index(drop=True)
            .set_index("uniqueID")
        )
        self.drain = profiles.drain
        self.charge_power = profiles.charge_power
        self.uncontrolled_charge = profiles.uncontrolled_charge
        self.max_battery_level = profiles.max_battery_level
        self.min_battery_level = profiles.min_battery_level
        self.aggregator = Aggregator(
            activities=self.activities,
            dataset=self.dataset,
            user_config=self.user_config,
            dev_config=self.dev_config,
            weighted=self.weighted,
            alpha=self.alpha,
        )

    def aggregate_profiles(self):
        self.drain_weekly = self.aggregator.perform_aggregation(profile=self.drain, pname="drain", method="flow")
        self.charge_power_weekly = self.aggregator.perform_aggregation(
            profile=self.charge_power, pname="charge_power", method="flow"
        )
        self.uncontrolled_charge_weekly = self.aggregator.perform_aggregation(
            profile=self.uncontrolled_charge, pname="uncontrolled_charge", method="flow"
        )
        self.max_battery_level_weekly = self.aggregator.perform_aggregation(
            profile=self.max_battery_level, pname="max_battery_level", method="state"
        )
        self.min_battery_level_weekly = self.aggregator.perform_aggregation(
            profile=self.min_battery_level, pname="min_battery_level", method="state"
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

    def __basic_aggregation(self, by_column: str = "tripStartWeekday") -> pd.Series:
        self.weekday_profiles = pd.DataFrame(columns=self.profile.columns, index=range(1, 8))
        cols = ["uniqueID", "tripWeight"] + [by_column]
        self.activities_subset = (
            self.activities[cols].copy().drop_duplicates(subset=["uniqueID"]).reset_index(drop=True)
        )
        self.activities_weekday = pd.merge(self.profile, self.activities_subset, on="uniqueID", how="inner")
        # self.profile.drop('uniqueID', axis=1, inplace=True)
        self.activities_weekday = self.activities_weekday.set_index("uniqueID")
        # Compose weekly profile from 7 separate profiles
        if self.method == "flow":
            if self.weighted:
                self.__weighted_mean_flow_profiles(by_column="tripStartWeekday")
            else:
                self.__calculate_average_flow_profiles(by_column="tripStartWeekday")
        elif self.method == "state":
            self.__agg_state_profiles(by_column="tripStartWeekday", alpha=self.alpha)

    def __calculate_average_flow_profiles(self, by_column: str):
        for idate in self.activities_weekday[by_column].unique():
            weekday_subset = self.activities_weekday[self.activities_weekday[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop(columns=["tripStartWeekday", "tripWeight"], axis=1)
            weekday_subset_agg = weekday_subset.mean(axis=0)
            self.weekday_profiles.iloc[idate - 1] = weekday_subset_agg

    def __weighted_mean_flow_profiles(self, by_column: str):
        for idate in self.activities_weekday[by_column].unique():
            weekday_subset = self.activities_weekday[self.activities_weekday[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop("tripStartWeekday", axis=1)
            # aggregate activities_weekday to one profile by multiplying by weights
            weight_sum = sum(weekday_subset.tripWeight)
            weekday_subset_weight = weekday_subset.apply(lambda x: x * weekday_subset.tripWeight.values)
            weekday_subset_weight = weekday_subset_weight.drop("tripWeight", axis=1)
            weekday_subset_weight_agg = weekday_subset_weight.sum() / weight_sum
            self.weekday_profiles.iloc[idate - 1] = weekday_subset_weight_agg

    def __agg_state_profiles(self, by_column: str, alpha: int = 10):
        """
        Selects the alpha (100 - alpha) percentile from maximum battery level
        (minimum batttery level) profile for each hour. If alpha = 10, the
        10%-biggest (10%-smallest) value is selected, all values beyond are
        disregarded as outliers.

        :param by_column: Currently tripWeekday
        :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be
            fulfilled after selection.
        :return: No return. Result is written to self.weekday_profiles with bins
            in the columns and weekday identifiers in the rows.
        """
        for idate in self.activities_weekday[by_column].unique():
            levels = self.activities_weekday.copy()
            weekday_subset = levels[levels[by_column] == idate].reset_index(drop=True)
            weekday_subset = weekday_subset.drop(columns=["tripStartWeekday", "tripWeight"])
            weekday_subset = weekday_subset.convert_dtypes()
            if self.pname == "max_battery_level":
                self.weekday_profiles.iloc[idate - 1] = weekday_subset.quantile(1 - (alpha / 100))
            elif self.pname == "min_battery_level":
                self.weekday_profiles.iloc[idate - 1] = weekday_subset.quantile(alpha / 100)
            else:
                raise NotImplementedError(f"An unknown profile {self.pname} was selected.")

    def __compose_week_profile(self):
        # input is self.weekday_profiles
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
        if self.user_config["global"]["writeOutputToDisk"]["aggregatorOutput"]:
            root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
            folder = self.dev_config["global"]["pathRelative"]["aggregatorOutput"]
            fileName = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manualLabel=self.pname,
                fileNameID="outputProfileAggregator",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / fileName)

    def perform_aggregation(self, profile: pd.DataFrame, pname: str, method: str) -> pd.DataFrame:
        self.profile = profile
        self.pname = pname
        self.method = method
        print(f"Starting to aggregate {self.pname} to fleet level based on day of the week.")
        start_time_agg = time.time()
        self.__basic_aggregation()
        self.__compose_week_profile()
        if self.user_config["global"]["writeOutputToDisk"]["aggregatorOutput"]:
            self.__write_output()
        print(f"Aggregation finished for {self.pname}.")
        elapsed_time_agg = time.time() - start_time_agg
        print(f"Needed time to aggregate {self.pname}: {elapsed_time_agg}.")
        self.profile = None
        self.pname = None
        self.method = None
        return self.weekly_profile
