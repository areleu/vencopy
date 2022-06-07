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
from vencopy.scripts.globalFunctions import loadConfigDict
from vencopy.classes.dataParsers import ParseMiD, ParseKiD, ParseVF


class GridModeler:
    def __init__(self, configDict: dict, datasetID: str, activities, gridModel: str):
        self.globalConfig = configDict['globalConfig']
        self.gridConfig = configDict['gridConfig']
        self.flexConfig = configDict['flexConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.gridModel = gridModel
        self.activities = activities.data
        self.gridAvailabilitySimple = self.gridConfig['chargingInfrastructureMappings']
        self.gridAvailabilityProb = self.gridConfig['gridAvailabilityDistribution']
        self.chargeAvailability = None
        self.calcGrid()

    def assignGridViaPurposes(self):
        """
        Method to translate hourly purpose profiles into hourly profiles of
        true/false giving the charging station
        availability in each hour for each individual vehicle.

        :return: None
        """
        print('Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.activities.purposeStr.replace(self.gridAvailabilitySimple)
        self.chargeAvailability = (~(self.chargeAvailability != True))  # FIXME: why?
        self.chargeAvailability = self.chargeAvailability * self.gridConfig['ratedPowerSimple']
        print('Grid connection assignment complete')

    def appendGridAvailability(self):
        """
        Function to write out the boolean charging station availability for
        each vehicle in each hour to the output file path.
        """
        self.activities['chargingPower'] = self.chargeAvailability

    def assignGridViaProbabilities(self, setSeed: int):
        """
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip
                 assigned with probability distribution
        """
        newActivities = []
        print('Starting with charge connection replacement of location purposes')
        for purpose in self.activities.purposeStr.unique():
            subset = self.activities.loc[self.activities.purposeStr == purpose].copy()
            power = list((self.gridConfig["gridAvailabilityDistribution"][purpose]).keys())
            probability = list(self.gridConfig["gridAvailabilityDistribution"][purpose].values())
            urng = np.random.default_rng(setSeed)  # universal non-uniform non random number
            rng = DiscreteAliasUrn(probability, random_state=urng)
            self.chargeAvailability = rng.rvs(len(subset))
            self.chargeAvailability = [power[i] for i in self.chargeAvailability]
            subset.loc[:, ("chargingPower")] = self.chargeAvailability
            newActivities.append(subset)
        self.activities = pd.concat(newActivities)
        print('Grid connection assignment complete')

    def calcGrid(self):
        """
        Wrapper function for grid assignment. The number of iterations for
        assignGridViaProbabilities() and transactionStartHour() and seed for
        reproduction of random numbers can be specified here.
        """
        if self.gridModel == 'simple':
            self.assignGridViaPurposes()
            self.appendGridAvailability()
        elif self.gridModel == 'probability':
            self.assignGridViaProbabilities(setSeed=42)
            # FIXME: add condition that charging at home in the morning has the same rated capacity as in the evening
            # FIXME: if first and/or last parking ar at home, reiterate the home distribution (or separate home from
            # the main function)
        else:
            raise(ValueError(f'Specified grid modeling option {self.gridModel} is not implemented. Please choose'
                             f'"simple" or "probability"'))

if __name__ == "__main__":

    from vencopy.scripts.globalFunctions import loadConfigDict

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "tripConfig",
        "gridConfig",
        "flexConfig",
        "evaluatorConfig",
    )
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData, gridModel='probability')
