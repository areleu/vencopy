__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
from scripts.libInput import *

# FIXME Add distributions and charging power ratings


def assignSimpleChargeInfra(config, dataset='MiD17'):
    # Processing MID file resulting from Christoph Schimeczeks SQL-Parsing of MID2008
    # MiD08 Purposes
    print('Starting with charge connection of MiD08')
    linkPurposes = Path(config['linksRelative']['input']) / config['files']['MiD08']['purposesProcessed']
    purposeDayData_raw = pd.read_excel(linkPurposes, sheet_name='Places')
    chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributionsSchimmy'])
    chargeAvailability.set_index(['VEHICLE', 'Day', 'Weight'], inplace=True)
    chargeAvailability.iloc[~chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
    chargeAvailability.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Plug_MiD08.csv')
    chargeAvailability = chargeAvailability.reset_index([1, 2])

    # MiD08 Trips
    print('Starting with trip weight concatenation of MiD08')
    linkTrips = Path(config['linksRelative']['input']) / config['files']['MiD08']['tripsProcessed']
    tripDayData_raw = pd.read_excel(linkTrips, sheet_name='DistancesInKm')
    tripData = tripDayData_raw.set_index('VEHICLE')
    tripData = pd.concat([tripData, chargeAvailability.loc[:, ['Day', 'Weight']]], axis=1)
    tripData.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Drive_MiD08.csv')

    # MiD17 Purposes
    print('Starting with charge connection replacement of MiD17')
    purposeDayData_raw = pd.read_csv(Path(config['linksRelative']['input']) / config['files'][dataset]['purposesProcessed'])
    chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributions'])
    filenameOut = 'inputProfiles_Plug_runTest_' + dataset + '.csv'
    chargeAvailability.to_csv(Path(config['linksRelative']['input']) / filenameOut)
    print('end')

if __name__ == '__main__':
    linkConfig = Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    assignSimpleChargeInfra(config=config)
