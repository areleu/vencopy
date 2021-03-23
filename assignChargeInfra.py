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
        # Next two lines needed?
        self.chargeAvailability.set_index(['hhPersonID', 'tripStartWeekday'], inplace=True)
        self.chargeAvailability.iloc[~self.chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
        print('Grid connection assignment complete')

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

def assignSimpleChargeInfra(config, dataset='MiD17'):
    # Processing MID file resulting from Christoph Schimeczeks SQL-Parsing of MID2008
    # MiD08 Purposes
    # print('Starting with charge connection of MiD08')
    # linkPurposes = Path(config['linksRelative']['input']) / config['files']['MiD08']['purposesProcessed']
    # purposeDayData_raw = pd.read_excel(linkPurposes, sheet_name='Places')
    # chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributionsSchimmy'])
    # chargeAvailability.set_index(['VEHICLE', 'Day', 'Weight'], inplace=True)
    # chargeAvailability.iloc[~chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
    # chargeAvailability.to_csv(Path(config['linksRelative']['input']) / config['files'][dataset]['inputDataPlugProfiles'])
    # chargeAvailability = chargeAvailability.reset_index([1, 2])

    # MiD08 Trips
    # print('Starting with trip weight concatenation of MiD08')
    # linkTrips = Path(config['linksRelative']['input']) / config['files']['MiD08']['tripsProcessed']
    # tripDayData_raw = pd.read_excel(linkTrips, sheet_name='DistancesInKm')
    # tripData = tripDayData_raw.set_index('VEHICLE')
    # tripData = pd.concat([tripData, chargeAvailability.loc[:, ['Day', 'Weight']]], axis=1)
    # tripData.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Drive_MiD08.csv')

    # MiD17 Purposes
    print(f'Starting with charge connection replacement of {dataset}')
    purposeFileName = createFileString(config=config, fileKey='purposesProcessed', dataset=dataset)
    purposeDayData_raw = pd.read_csv(Path(config['linksRelative']['input']) / purposeFileName, keep_default_na=False)
    chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributions'])
    chargeAvailability.to_csv(Path(config['linksRelative']['input']) /
                              createFileString(config=config, fileKey='inputDataPlugProfiles', dataset=dataset))
    print('end')

if __name__ == '__main__':
    linkConfig = Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # assignSimpleChargeInfra(config=config, dataset='MiD08')
    vpg = vpGrid(config=config, dataset='MiD08')
    vpg.assignSimpleGridViaPurposes()
    vpg.writeOutGridAvailability()
