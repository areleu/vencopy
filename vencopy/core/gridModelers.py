__version__ = "0.1.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path
import pandas as pd
import numpy as np
from scipy.stats.sampling import DiscreteAliasUrn
from vencopy.utils.globalFunctions import createFileName, loadConfigDict, writeOut
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF


class GridModeler:
    def __init__(self, configDict: dict, datasetID: str, activities, gridModel: str, forceLastTripHome: bool = False):
        self.globalConfig = configDict['globalConfig']
        self.gridConfig = configDict['gridConfig']
        self.flexConfig = configDict['flexConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.gridModel = gridModel
        self.activities = activities
        if forceLastTripHome:
            self.removeActsNotEndingHome()
        self.gridAvailabilitySimple = self.gridConfig['chargingInfrastructureMappings']
        self.gridAvailabilityProb = self.gridConfig['gridAvailabilityDistribution']
        self.chargeAvailability = None

    def _assignGridViaPurposes(self):
        """
        Method to translate purpose profiles into hourly profiles of
        true/false giving the charging station
        availability for each individual vehicle.

        :return: None
        """
        print('Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.activities.purposeStr.replace(self.gridAvailabilitySimple)
        # self.chargeAvailability = (~(self.chargeAvailability != True))  # check condition if not, needed?
        self.chargeAvailability = self.chargeAvailability * self.gridConfig['ratedPowerSimple']
        self.activities['ratedPower'] = self.chargeAvailability
        self.activities = self.adjustPowerShortParking()
        print('Grid connection assignment complete')

    def _assignGridViaProbabilities(self, setSeed: int):
        """
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip
                 assigned with probability distribution
        """
        activitiesNoHome = []
        print('Starting with charge connection replacement of location purposes')
        for purpose in self.activities.purposeStr.unique():
            if purpose == "HOME":
                activitiesHome = self._homeProbabilityDistribution(setSeed=42)
            else:
                subset = self.activities.loc[self.activities.purposeStr == purpose].copy()
                power = list((self.gridConfig["gridAvailabilityDistribution"][purpose]).keys())
                probability = list(self.gridConfig["gridAvailabilityDistribution"][purpose].values())
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
        print('Grid connection assignment complete')

    def _homeProbabilityDistribution(self, setSeed: int):
        # adds condition that charging at home in the morning has the same rated capacity as in the evening
        # if first and/or last parking ar at home, instead of reiterating the home distribution (or separate home from
        # the main function) it assign the home charging probability based on unique household IDs instead of
        # dataset entries -> each HH always has same rated power
        purpose = "HOME"
        homeActivities = self.activities.loc[self.activities.purposeStr == purpose].copy()
        households = homeActivities[['hhID']].reset_index(drop=True)
        households = households.drop_duplicates(subset="hhID").copy()  # 73850 unique HH
        power = list((self.gridConfig["gridAvailabilityDistribution"][purpose]).keys())
        probability = list(self.gridConfig["gridAvailabilityDistribution"][purpose].values())
        urng = np.random.default_rng(setSeed)  # universal non-uniform random number
        rng = DiscreteAliasUrn(probability, random_state=urng)
        self.chargeAvailability = rng.rvs(len(households))
        self.chargeAvailability = [power[i] for i in self.chargeAvailability]
        households.loc[:, ("ratedPower")] = self.chargeAvailability
        households.set_index("hhID", inplace=True)
        homeActivities = homeActivities.join(households, on="hhID")
        return homeActivities

    def adjustPowerShortParking(self):
        """
        Adjusts charging power to zero if parking duration shorter than 15 minutes.
        """
        # parkID != pd.NA and timedelta <= 15 minutes
        self.activities.loc[((self.activities['parkID'].notna()) & ((self.activities['timedelta'] / np.timedelta64(1, 's')) <= self.gridConfig['minimumParkingTime'])), 'ratedPower'] = 0
        return self.activities

    def assignGrid(self, losses: bool = False):
        """
        Wrapper function for grid assignment. The number of iterations for
        assignGridViaProbabilities() and transactionStartHour() and seed for
        reproduction of random numbers can be specified here.

        :param losses [bool]: Should electric losses in the charging equipment be considered?
        """
        if self.gridModel == 'simple':
            self._assignGridViaPurposes()
        elif self.gridModel == 'probability':
            seed = 42
            self._assignGridViaProbabilities(setSeed=seed)
        else:
            raise(ValueError(f'Specified grid modeling option {self.gridModel} is not implemented. Please choose'
                             f'"simple" or "probability"'))

        self.activities = self._addGridLosses(acts=self.activities, loss=self.gridConfig['loss_factor'], losses=losses)
        return self.activities

    def _addGridLosses(self, acts: pd.DataFrame, loss: dict, losses: bool):
        """ Function applying a reduction of rated power capacities to the rated powers after sampling. The 
        factors for reducing the rated power are given in the gridConfig with keys being floats of rated powers
        and values being floats between 0 and 1. The factor is the LOSS FACTOR not the EFFICIENCY, thus 0.1 applied to
        a rated power of 11 kW will yield an available power of 9.9 kW.

        :param acts [bool]: Should electric losses in the charging equipment be considered?
        :param losses [bool]: Should electric losses in the charging equipment be considered?
        """
        if losses:
            acts['availablePower'] = acts['ratedPower'] - acts['ratedPower'] * acts['ratedPower'].apply(
                lambda x: loss[x])
        else:
            acts['availablePower'] = acts['ratedPower']
        return acts

    def writeOutput(self):
        root = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot'])
        folder = self.globalConfig['pathRelative']['gridOutput']
        fileName = createFileName(globalConfig=self.globalConfig, manualLabel='', file='outputGridModeler',
                                    datasetID=self.datasetID)
        writeOut(data = self.activities, path = root / folder / fileName)

    def removeActsNotEndingHome(self):
        lastActsNotHome = self.activities.loc[(self.activities['purposeStr'] != 'HOME') & (
            self.activities['isLastActivity']), :]
        idToRemove = lastActsNotHome['genericID'].unique()
        self.activities = self.activities.loc[~self.activities['genericID'].isin(idToRemove), :]

if __name__ == "__main__":

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(
        configDict=configDict,
        datasetID=datasetID,
        activities=vpData.activities,
        gridModel='simple',
        forceLastTripHome=True)
    vpGrid.assignGrid()
    vpGrid.writeOutput()
