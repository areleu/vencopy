__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.07.2022"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import numpy as np
from scipy.stats.sampling import DiscreteAliasUrn
from vencopy.utils.globalFunctions import create_file_name, write_out


class GridModeler:
    def __init__(self, configs: dict, activities):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = configs["user_config"]["global"]["dataset"]
        self.grid_model = self.user_config["gridModelers"]["gridModel"]
        self.activities = activities
        if self.user_config["gridModelers"]["forceLastTripHome"]:
            self.__remove_activities_not_ending_home()
        self.grid_availability_simple = self.user_config["gridModelers"]["chargingInfrastructureMappings"]
        self.grid_availability_probability = self.user_config["gridModelers"]["gridAvailabilityDistribution"]
        self.charging_availability = None

    def __assign_grid_via_purposes(self):
        """
        Method to translate purpose profiles into hourly profiles of
        true/false giving the charging station
        availability for each individual vehicle.

        :return: None
        """
        print("Starting with charge connection replacement of location purposes.")
        self.charging_availability = self.activities.purpose_string.replace(self.grid_availability_simple)
        # self.charging_availability = (~(self.charging_availability != True))  # check condition if not, needed?
        self.charging_availability = self.charging_availability * self.user_config["gridModelers"]["ratedPowerSimple"]
        self.activities["ratedPower"] = self.charging_availability
        self.activities = self.__adjust_power_short_parking_time()
        print("Grid connection assignment complete.")

    def __assign_grid_via_probabilities(self, setSeed: int):
        """
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip
                 assigned with probability distribution
        """
        activitiesNoHome = []
        print("Starting with charge connection replacement of location purposes.")
        for purpose in self.activities.purpose_string.unique():
            if purpose == "HOME":
                activitiesHome = self.__home_probability_distribution(setSeed=42)
            else:
                subset = self.activities.loc[self.activities.purpose_string == purpose].copy()
                power = list((self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose]).keys())
                probability = list(self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose].values())
                urng = np.random.default_rng(setSeed)  # universal non-uniform random number
                rng = DiscreteAliasUrn(probability, random_state=urng)
                self.charging_availability = rng.rvs(len(subset))
                self.charging_availability = [power[i] for i in self.charging_availability]
                subset.loc[:, ("ratedPower")] = self.charging_availability
                activitiesNoHome.append(subset)
        activitiesNoHome = pd.concat(activitiesNoHome).reset_index(drop=True)
        dataframes = [activitiesHome, activitiesNoHome]
        self.activities = pd.concat(dataframes).reset_index(drop=True)
        self.activities = self.__adjust_power_short_parking_time()
        print("Grid connection assignment complete.")

    def __home_probability_distribution(self, setSeed: int) -> pd.DataFrame:
        """Adds condition that charging at home in the morning has the same rated capacity as in the evening
        if first and/or last parking ar at home, instead of reiterating the home distribution (or separate home from
        the main function) it assign the home charging probability based on unique household IDs instead of
        dataset entries -> each HH always has same rated power

        Args:
            setSeed (int): Optional argument for sampling of household rated powers.

        Returns:
            pd.DataFrame: Activity data set with sampled and harmonized home charging rated powers.
        """

        purpose = "HOME"
        homeActivities = self.activities.loc[self.activities.purpose_string == purpose].copy()
        households = homeActivities[["household_id"]].reset_index(drop=True)
        households = households.drop_duplicates(subset="household_id").copy()  # 73850 unique HH
        power = list((self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose]).keys())
        probability = list(self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose].values())
        urng = np.random.default_rng(setSeed)  # universal non-uniform random number
        rng = DiscreteAliasUrn(probability, random_state=urng)
        self.charging_availability = rng.rvs(len(households))
        self.charging_availability = [power[i] for i in self.charging_availability]
        households.loc[:, ("ratedPower")] = self.charging_availability
        households.set_index("household_id", inplace=True)
        homeActivities = homeActivities.join(households, on="household_id")
        return homeActivities

    def __adjust_power_short_parking_time(self) -> pd.DataFrame:
        """
        Adjusts charging power to zero if parking duration shorter than 15 minutes.
        """
        # park_id != pd.NA and timedelta <= 15 minutes
        self.activities.loc[
            (
                (self.activities["park_id"].notna())
                & (
                    (self.activities["timedelta"] / np.timedelta64(1, "s"))
                    <= self.user_config["gridModelers"]["minimumParkingTime"]
                )
            ),
            "ratedPower",
        ] = 0
        return self.activities

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
            self.__assign_grid_via_probabilities(setSeed=seed)
        else:
            raise (
                ValueError(
                    f"Specified grid modeling option {self.grid_model} is not implemented. Please choose"
                    f'"simple" or "probability"'
                )
            )
        self.__add_grid_losses()
        self.__writeOutput()
        return self.activities

    def __add_grid_losses(self) -> pd.DataFrame:
        """
        Function applying a reduction of rated power capacities to the rated powers after sampling. The
        factors for reducing the rated power are given in the gridConfig with keys being floats of rated powers
        and values being floats between 0 and 1. The factor is the LOSS FACTOR not the EFFICIENCY, thus 0.1 applied to
        a rated power of 11 kW will yield an available power of 9.9 kW.

        :param acts [bool]: Should electric losses in the charging equipment be considered?
        :param losses [bool]: Should electric losses in the charging equipment be considered?
        """
        if self.user_config["gridModelers"]["losses"]:
            self.activities["availablePower"] = self.activities["ratedPower"] - (
                self.activities["ratedPower"]
                * self.activities["ratedPower"].apply(
                    lambda x: self.user_config["gridModelers"]["loss_factor"][f"rated_power_{str(x)}"]
                )
            )
        else:
            self.activities["availablePower"] = self.activities["ratedPower"]
        return self.activities

    def __writeOutput(self):
        if self.user_config["global"]["write_output_to_disk"]["gridOutput"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["gridOutput"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manual_label="",
                file_name_id="outputGridModeler",
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
