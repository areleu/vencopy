__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte, Ronald Stegen'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
import pandas as pd
import yaml
import os
from vencopy.scripts.globalFunctions import createFileString


class GridModeler:
    def __init__(self, gridConfig: dict, globalConfig: dict, datasetID: str ='MiD17'):
        """
        Class for modeling individual vehicle connection options dependent on parking purposes. Configurations on
        charging station availabilities can be parametrized in gridConfig. globalConfig and datasetID are needed for
        reading the input files.

        :param gridConfig: Dictionary containing a key chargingInfrastructureMapping with a sub-dictionary mapping the
        relevant parking purposes to grid availability (true/false). The gridConfig will contain dictionaries with
        probabilistic grid availabilities per parking purpose and rated charging power.
        :param globalConfig: Dictionary with relative paths and filenames. Used for referencing the purpose input file
        :param datasetID: String, used for referencing the purpose input file
        """

        self.inputFileName = createFileString(globalConfig=globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(globalConfig['pathRelative']['input']) / self.inputFileName
        self.gridDistributions = gridConfig['chargingInfrastructureMappings']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['pathRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.chargeAvailability = None

    def assignSimpleGridViaPurposes(self):
        """
        Method to translate hourly purpose profiles into hourly profiles of true/false giving the charging station
        availability in each hour for each individual vehicle.

        :return: None
        """
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridDistributions)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    def writeOutGridAvailability(self):
        """
        Function to write out the boolean charging station availability for each vehicle in each hour to the output
        file path.

        :return: None
        """

        self.chargeAvailability.to_csv(self.outputFilePath)


if __name__ == '__main__':
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGridConfig = Path.cwd().parent / 'config' / 'gridConfig.yaml'
    with open(pathGridConfig) as ipf:
        gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    os.chdir(localPathConfig['pathAbsolute']['vencoPyRoot'])
    vpg = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig)
    vpg.assignSimpleGridViaPurposes()
    vpg.writeOutGridAvailability()
