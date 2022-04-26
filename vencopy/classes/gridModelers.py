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
from scipy.stats import beta, gamma
import matplotlib.pyplot as plt
from vencopy.scripts.globalFunctions import createFileString, loadConfigDict
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
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.activities.purposeStr.replace(self.gridAvailabilitySimple)
        self.chargeAvailability = (~(self.chargeAvailability != True)) #FIXME: why?
        self.chargeAvailability = self.chargeAvailability * self.gridConfig['ratedPowerSimple']
        print('Grid connection assignment complete')

    def allocatePowerViaProbabilities(self, purpose: pd.Series, gridAvailability: dict):
        """
        Not tested, preleiminary version

        Assigns a random number between 0 and 1 for all the purposes, and
        allots a charging station according to the
        probability distribution

        :param purpose: Purpose of each trip
        :param gridAvailability: Dictionary specifying the probability of
                                 different charging powers at different parking
                                 purposes
        :return: Returns a charging capacity for a purpose
        """
        for genericID, tripPurpose in purpose.items():
            rnd = np.random.random_sample()
            prob_min = 0
            for index, (iPow, iProb) in enumerate(
                gridAvailability[tripPurpose].items()
            ):
                prob_max = prob_min + iProb
                if prob_min <= rnd <= prob_max:
                    power = iPow
                    purpose.loc[genericID] = power
                    break
                prob_min = prob_max
        return purpose


    def assignGridViaProbabilities(self, setSeed: int):
        """
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip
                 assigned with probability distribution
        """
        self.chargeAvailability = self.activities.purposeStr.replace(self.gridAvailabilitySimple)
        self.chargeAvailability = self.chargeAvailability.astype(int)
        self.purpose = self.activities.purposeStr
        print("Starting with charge connection replacement ")
        np.random.seed(setSeed)

        for i in range(len(self.chargeAvailability)):
            self.chargeAvailability.iloc[i] = self.allocatePowerViaProbabilities(
                purpose=self.purpose,
                gridAvailability=self.gridAvailabilityProb)
        print("Grid connection assignment complete")
        return self.chargeAvailability

    def appendGridAvailability(self):
        """
        Function to write out the boolean charging station availability for
        each vehicle in each hour to the output file path.
        
        :return: None
        """
        self.activities.chargingPower = self.chargeAvailability


    def calcGrid(self):
        """
        Wrapper function for grid assignment. The number of iterations for
        assignGridViaProbabilities() and transactionStartHour() and seed for
        reproduction of random numbers can be specified here.
        """
        if self.gridModel == 'simple':
            self.assignGridViaPurposes()
        elif self.gridModel == 'probability':
            self.assignGridViaProbabilities(setSeed=42)
        else:
            raise(ValueError(f'Specified grid modeling option {self.gridModel} is not implemented. Please choose'
                             f'"simple" or "probability"'))
        self.appendGridAvailability()


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
