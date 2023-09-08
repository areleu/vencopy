__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import numpy as np
from scipy.stats.sampling import DiscreteAliasUrn
from ..utils.utils import create_file_name, write_out


class GridModeler:
    def __init__(self, configs: dict, activities):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.grid_model = self.user_config["gridmodelers"]["grid_model"]
        self.activities = activities
        if self.user_config["gridmodelers"]["force_last_trip_home"]:
            self.__remove_activities_not_ending_home()
        self.grid_availability_simple = self.user_config["gridmodelers"]["charging_infrastructure_mappings"]
        self.grid_availability_probability = self.user_config["gridmodelers"]["grid_availability_distribution"]
        self.charging_availability = None

    def __assign_grid_via_purposes(self):
        """
        Assigns the grid connection power using the trip purposes though a true/false mapping in the user_config.yaml
        to represent the charging station availability for each individual vehicle.

        :return: None
        """
        print("Starting with charge connection replacement of location purposes.")
        self.charging_availability = self.activities.purpose_string.replace(self.grid_availability_simple)
        self.charging_availability = self.charging_availability * self.user_config["gridmodelers"]["rated_power_simple"]
        self.activities["rated_power"] = self.charging_availability
        self.__adjust_power_short_parking_time()
        print("Grid connection assignment complete.")

    def __assign_grid_via_probabilities(self, set_seed: int):
        """
        Assigns the grid usig probability distributions defined in user_config.yaml.

        :param set_seed: Seed for reproducing random number
        :return: None
        """
        activities_no_home = []
        print("Starting with charge connection replacement of location purposes.")
        for purpose in self.activities.purpose_string.unique():
            if purpose == "HOME":
                activities_home = self.__home_probability_distribution(set_seed=42)
            else:
                subset = self.activities.loc[self.activities.purpose_string == purpose].copy()
                power = list((self.user_config["gridmodelers"]["grid_availability_distribution"][purpose]).keys())
                probability = list(self.user_config["gridmodelers"]["grid_availability_distribution"][purpose].values())
                urng = np.random.default_rng(set_seed)  # universal non-uniform random number
                rng = DiscreteAliasUrn(probability, random_state=urng)
                self.charging_availability = rng.rvs(len(subset))
                self.charging_availability = [power[i] for i in self.charging_availability]
                subset.loc[:, ("rated_power")] = self.charging_availability
                activities_no_home.append(subset)
        activities_no_home = pd.concat(activities_no_home).reset_index(drop=True)
        dataframes = [activities_home, activities_no_home]
        self.activities = pd.concat(dataframes).reset_index(drop=True)
        self.__adjust_power_short_parking_time()
        print("Grid connection assignment complete.")

    def __home_probability_distribution(self, set_seed: int) -> pd.DataFrame:
        """
        Adds condition that charging at home in the morning has the same rated capacity as in the evening
        if first and/or last parking ar at home, instead of reiterating the home distribution (or separate home from
        the main function) it assign the home charging probability based on unique household IDs instead of
        dataset entries -> each HH always has same rated power

        Args:
            set_seed (int): Optional argument for sampling of household rated powers.

        Returns:
            pd.DataFrame: Activity data set with sampled and harmonized home charging rated powers.
        """
        purpose = "HOME"
        home_activities = self.activities.loc[self.activities.purpose_string == purpose].copy()
        households = home_activities[["household_id"]].reset_index(drop=True)
        households = households.drop_duplicates(subset="household_id").copy()  # 73850 unique HH
        power = list((self.user_config["gridmodelers"]["grid_availability_distribution"][purpose]).keys())
        probability = list(self.user_config["gridmodelers"]["grid_availability_distribution"][purpose].values())
        urng = np.random.default_rng(set_seed)  # universal non-uniform random number
        rng = DiscreteAliasUrn(probability, random_state=urng)
        self.charging_availability = rng.rvs(len(households))
        self.charging_availability = [power[i] for i in self.charging_availability]
        households.loc[:, ("rated_power")] = self.charging_availability
        households.set_index("household_id", inplace=True)
        home_activities = home_activities.join(households, on="household_id")
        return home_activities

    def __adjust_power_short_parking_time(self) -> pd.DataFrame:
        """
        Adjusts charging power to zero if parking duration shorter than 15 minutes.
        """
        # park_id != pd.NA and time_delta <= 15 minutes
        self.activities.loc[
            (
                (self.activities["park_id"].notna())
                & (
                    (self.activities["time_delta"] / np.timedelta64(1, "s"))
                    <= self.user_config["gridmodelers"]["minimum_parking_time"]
                )
            ),
            "rated_power",
        ] = 0

    def __add_grid_losses(self) -> pd.DataFrame:
        """
        Function applying a reduction of rated power capacities to the rated powers after sampling. The
        factors for reducing the rated power are given in the gridConfig with keys being floats of rated powers
        and values being floats between 0 and 1. The factor is the LOSS FACTOR not the EFFICIENCY, thus 0.1 applied to
        a rated power of 11 kW will yield an available power of 9.9 kW.

        :param activities [bool]: Should electric losses in the charging equipment be considered?
        :param losses [bool]: Should electric losses in the charging equipment be considered?
        """
        if self.user_config["gridmodelers"]["losses"]:
            self.activities["available_power"] = self.activities["rated_power"] - (
                self.activities["rated_power"]
                * self.activities["rated_power"].apply(
                    lambda x: self.user_config["gridmodelers"]["loss_factor"][f"rated_power_{str(x)}"]
                )
            )
        else:
            self.activities["available_power"] = self.activities["rated_power"]

    def __writeOutput(self):
        if self.user_config["global"]["write_output_to_disk"]["grid_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["grid_output"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manual_label="",
                file_name_id="output_gridModeler",
                dataset=self.dataset,
            )
            write_out(data=self.activities, path=root / folder / file_name)

    def __remove_activities_not_ending_home(self):
        if self.dataset in ["MiD17", "VF"]:
            lastActsNotHome = self.activities.loc[
                (self.activities["purpose_string"] != "HOME") & (self.activities["is_last_activity"]), :
            ].copy()
            id_to_remove = lastActsNotHome["unique_id"].unique()
            self.activities = self.activities.loc[~self.activities["unique_id"].isin(id_to_remove), :].copy()

    def assign_grid(self, seed: int = 42) -> pd.DataFrame:
        """
        Wrapper function for grid assignment. The number of iterations for
        assignGridViaProbabilities() and seed for
        reproduction of random numbers can be specified here.
        """
        if self.grid_model == "simple":
            self.__assign_grid_via_purposes()
        elif self.grid_model == "probability":
            seed = seed
            self.__assign_grid_via_probabilities(set_seed=seed)
        else:
            raise (
                ValueError(
                    f"Specified grid modeling option {self.grid_model} is not implemented. Please choose"
                    f'"simple" or "probability".'
                )
            )
        self.__add_grid_losses()
        self.__writeOutput()