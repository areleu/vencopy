__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
import pandas as pd
import yaml
import os
from scripts.globalFunctions import createFileString

# FIXME Add distributions and charging power ratings

class GridModeler:
    def __init__(self, config: dict, globalConfig:dict, datasetID : str ='MiD17'):
        self.inputFileName = createFileString(globalConfig=globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(globalConfig['linksRelative']['input']) / self.inputFileName
        self.gridDistributions = config['chargingInfrastructureDistributions']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['linksRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)


    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridDistributions)

        # self.chargeAvailability.set_index(['hhPersonID', 'tripStartWeekday', 'tripWeight'], inplace=True)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        self.chargeAvailability.iloc[~self.chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
        print('Grid connection assignment complete')

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)


if __name__ == '__main__':
    linkGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    globalConfig = yaml.load(open(linkGlobalConfig), Loader=yaml.SafeLoader)
    linkGridConfig = Path.cwd().parent / 'config' / 'gridConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    gridConfig = yaml.load(open(linkGridConfig), Loader=yaml.SafeLoader)
    os.chdir(globalConfig['linksAbsolute']['vencoPyRoot'])

    vpg = GridModeler(config=gridConfig, globalConfig=globalConfig)
    vpg.assignSimpleGridViaPurposes()
    vpg.writeOutGridAvailability()
