__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
from scripts.libInput import *
from scripts.utilsParsing import createFileString

# FIXME Add distributions and charging power ratings

class vpGrid:
    def __init__(self, config, dataset='MiD17'):
        self.inputFileName = createFileString(config=config, fileKey='purposesProcessed', dataset=dataset)
        self.inputFilePath = Path(config['linksRelative']['input']) / self.inputFileName
        self.gridDistributions = config['chargingInfrastructureDistributions']
        self.outputFileName = createFileString(config=config, fileKey='inputDataPlugProfiles', dataset=dataset)
        self.outputFilePath = Path(config['linksRelative']['input']) / self.outputFileName

        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)


    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridDistributions)

        self.chargeAvailability.set_index(['hhPersonID', 'tripStartWeekday', 'tripWeight'], inplace=True)
        self.chargeAvailability.iloc[~self.chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
        print('Grid connection assignment complete')

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)


if __name__ == '__main__':
    linkConfig = Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    vpg = vpGrid(config=config, dataset='MiD17')
    vpg.assignSimpleGridViaPurposes()
    vpg.writeOutGridAvailability()
