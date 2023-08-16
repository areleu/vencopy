__version__ = "0.1.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import numpy as np
from scipy.stats.sampling import DiscreteAliasUrn
from vencopy.utils.globalFunctions import createFileName, writeOut


class GridModeler:
    def __init__(self, config_dict: dict, activities):
        self.user_config = config_dict["user_config"]
        self.dev_config = config_dict["dev_config"]
        self.datasetID = config_dict["user_config"]["global"]["dataset"]
        self.gridModel = self.user_config["gridModelers"]["gridModel"]
        self.activities = activities
        if self.user_config["gridModelers"]["forceLastTripHome"]:
            self.removeActsNotEndingHome()
        self.gridAvailabilitySimple = self.user_config["gridModelers"]["chargingInfrastructureMappings"]
        self.gridAvailabilityProb = self.user_config["gridModelers"]["gridAvailabilityDistribution"]
        self.chargeAvailability = None

    def _assignGridViaPurposes(self):
        """
        Method to translate purpose profiles into hourly profiles of
        true/false giving the charging station
        availability for each individual vehicle.

        :return: None
        """
        print("Starting with charge connection replacement of location purposes.")
        self.chargeAvailability = self.activities.purposeStr.replace(self.gridAvailabilitySimple)
        # self.chargeAvailability = (~(self.chargeAvailability != True))  # check condition if not, needed?
        self.chargeAvailability = self.chargeAvailability * self.user_config["gridModelers"]["ratedPowerSimple"]
        self.activities["ratedPower"] = self.chargeAvailability
        self.activities = self.adjustPowerShortParking()
        print("Grid connection assignment complete.")

    def _assignGridViaProbabilities(self, setSeed: int):
        """
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip
                 assigned with probability distribution
        """
        activitiesNoHome = []
        print("Starting with charge connection replacement of location purposes.")
        for purpose in self.activities.purposeStr.unique():
            if purpose == "HOME":
                activitiesHome = self._homeProbabilityDistribution(setSeed=42)
            else:
                subset = self.activities.loc[self.activities.purposeStr == purpose].copy()
                power = list((self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose]).keys())
                probability = list(self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose].values())
                urng = np.random.default_rng(setSeed)  # universal non-uniform random number
                rng = DiscreteAliasUrn(probability, random_state=urng)
                self.chargeAvailability = rng.rvs(len(subset))
                self.chargeAvailability = [power[i] for i in self.chargeAvailability]
                subset.loc[:, ("ratedPower")] = self.chargeAvailability
                activitiesNoHome.append(subset)
        activitiesNoHome = pd.concat(activitiesNoHome).reset_index(drop=True)
        dataframes = [activitiesHome, activitiesNoHome]
        self.activities = pd.concat(dataframes).reset_index(drop=True)
        self.activities = self.adjustPowerShortParking()
        print("Grid connection assignment complete.")

    def _homeProbabilityDistribution(self, setSeed: int) -> pd.DataFrame:
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
        homeActivities = self.activities.loc[self.activities.purposeStr == purpose].copy()
        households = homeActivities[["hhID"]].reset_index(drop=True)
        households = households.drop_duplicates(subset="hhID").copy()  # 73850 unique HH
        power = list((self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose]).keys())
        probability = list(self.user_config["gridModelers"]["gridAvailabilityDistribution"][purpose].values())
        urng = np.random.default_rng(setSeed)  # universal non-uniform random number
        rng = DiscreteAliasUrn(probability, random_state=urng)
        self.chargeAvailability = rng.rvs(len(households))
        self.chargeAvailability = [power[i] for i in self.chargeAvailability]
        households.loc[:, ("ratedPower")] = self.chargeAvailability
        households.set_index("hhID", inplace=True)
        homeActivities = homeActivities.join(households, on="hhID")
        return homeActivities

    def adjustPowerShortParking(self) -> pd.DataFrame:
        """
        Adjusts charging power to zero if parking duration shorter than 15 minutes.
        """
        # parkID != pd.NA and timedelta <= 15 minutes
        self.activities.loc[
            (
                (self.activities["parkID"].notna())
                & (
                    (self.activities["timedelta"] / np.timedelta64(1, "s"))
                    <= self.user_config["gridModelers"]["minimumParkingTime"]
                )
            ),
            "ratedPower",
        ] = 0
        return self.activities

    def assignGrid(self, seed: int = 42) -> pd.DataFrame:
        """
        Wrapper function for grid assignment. The number of iterations for
        assignGridViaProbabilities() and seed for
        reproduction of random numbers can be specified here.
        """
        if self.gridModel == "simple":
            self._assignGridViaPurposes()
        elif self.gridModel == "probability":
            seed = seed
            self._assignGridViaProbabilities(setSeed=seed)
        else:
            raise (
                ValueError(
                    f"Specified grid modeling option {self.gridModel} is not implemented. Please choose"
                    f'"simple" or "probability"'
                )
            )
        self._addGridLosses()
        self._writeOutput()
        return self.activities

    def _addGridLosses(self) -> pd.DataFrame:
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

    def _writeOutput(self):
        if self.user_config["global"]["writeOutputToDisk"]["gridOutput"]:
            root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
            folder = self.dev_config["global"]["pathRelative"]["gridOutput"]
            fileName = createFileName(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manualLabel="",
                fileNameID="outputGridModeler",
                datasetID=self.datasetID,
            )
            writeOut(data=self.activities, path=root / folder / fileName)

    def removeActsNotEndingHome(self):
        if self.datasetID in ["MiD17", "VF"]:
            lastActsNotHome = self.activities.loc[
                (self.activities["purposeStr"] != "HOME") & (self.activities["isLastActivity"]), :
            ].copy()
            idToRemove = lastActsNotHome["uniqueID"].unique()
            self.activities = self.activities.loc[~self.activities["uniqueID"].isin(idToRemove), :].copy()
