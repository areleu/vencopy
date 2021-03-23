__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '08.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
import pathlib
import time
from profilehooks import profile
from scripts.utilsParsing import *
from scripts.libPlotting import *
from scripts.libLogging import logger
from scripts.parseManager import ParseData


# ToDo: Hebefaktoren einrechnen bei nicht-detaillierter Erfassung (siehe MID-Nuterhandbuch, S. 33f.)

@profile(immediate=True)
def parseMiD(dataset: str, config: dict):
    p = ParseData(datasetID=dataset, config=config, strColumns=True, loadEncrypted=False)

    # # FIXME Currently, trips starting before 12:00 and ending at 13:00 have an endhour share of 0 and a fullHour. Change this to the respective share for two-hour trips

    # FIXME Currently, trips starting before 12:00 and ending at 13:00 have an endhour share of 0 and a fullHour. Change this to the respective share for two-hour trips
    tripDataWHourlyShares = calcHourlyShares(p.data, ts_st='timestampStart', ts_en='timestampEnd')

    # ToDo: Fix implausible trips where shareStartHour = NAN or a share with shareEndHour and noOfFullHours both =0
    tripDataClean = tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    print('Initiating allocation of driving distance to hours')
    emptyDF = initiateHourDataframe(tripDataClean.index, config['numberOfHours'])

    # Class instantiating of callable class
    fillHourValues = FillHourValues(data=tripDataClean, rangeFunction=initiateColRange)
    driveDataTrips = fillDataframe(emptyDF, fillFunction=fillHourValues)

    driveDataTrips.loc[:, ['hhPersonID', 'tripID']] = pd.DataFrame(tripDataClean.loc[:, ['hhPersonID', 'tripID']])
    driveDataDays = mergeTrips(driveDataTrips)
    print('Finished allocation of driving distance to hours')

    tripPurposesDriving = assignDriving(driveDataDays)
    print('Finished hourly dataframe replacements')

    print('Initiating trip purpose allocation')
    hhPersonMap = mapHHPIDToTripID(tripDataClean)
    purposeDataDays = fillDayPurposes(tripDataClean, tripPurposesDriving)
    purposeDataDays.replace({'0.0': 'HOME'})  # FIXME: This was a quick-fix, integrate into case-differentiation
    print('Finished purpose replacements')
    print(f'There are {len(purposeDataDays)} daily trip diaries.')

    #FIXME: Hard coded variable string not available anymore in both datasets
    indexedDriveData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['tripStartWeekday', 'tripWeight'])  # 'tripWeight', 'tripScaleFactor'
    print(purposeDataDays.head())
    print(tripDataClean.head())
    indexedPurposeData = mergeVariables(data=purposeDataDays, variableData=tripDataClean, variables=['tripStartWeekday', 'tripWeight'])  # 'tripWeight', 'tripScaleFactor'

    print(indexedDriveData.head())
    print(indexedPurposeData.head())
    writeOut(config=config, dataset=dataset, dataDrive=indexedDriveData, dataPurpose=indexedPurposeData)



if __name__ == '__main__':
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    parseMiD(dataset='MiD08', config=config)
