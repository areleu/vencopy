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


# ToDo: Hebefaktoren einrechnen bei nicht-detaillierter Erfassung (siehe MID-Nuterhandbuch, S. 33f.)

@profile(immediate=True)
def parseMiD(dataset, config):
    #----- data read-in -----
    # hhData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017households'], sep=';')
    # personData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017persons'], sep=';')
    # tripData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute'][dataset]) / config['files'][dataset]['tripsDataRaw'],
    #                            sep=';', decimal=',')
    tripData_raw = pd.read_stata(pathlib.Path(config['linksAbsolute'][dataset]) / config['files'][dataset]['tripsDataRaw'],
                                 convert_categoricals=False, convert_dates=False, preserve_dtypes=True)  # be explicit about column types

    tripDataHarmonVariables = harmonizeVariables(data=tripData_raw, dataset=dataset, config=config)
    relevantVariables = createListOfVariables(dataset=dataset, config=config)

    # W_VM_G=1 -> Only motorized individual vehicle trips
    tripData = tripDataHarmonVariables.loc[tripDataHarmonVariables.loc[:, 'isMIVDriver'] == 1, relevantVariables]
    # Dtype assignment
    tripData = tripData.astype(config['inputDTypes'])

    # Dataset filtering
    tripData = tripData.loc[(tripData['tripStartHour'] != 99) & (tripData['tripEndHour'] != 99), :]  # beginning and end hour must be available
    tripData = tripData.loc[(tripData['tripStartHour'] != 701) & (tripData['tripEndHour'] != 701), :]
    tripData = tripData.loc[(tripData['tripStartClock'] <= tripData['tripEndClock']) |
                            (tripData['tripEndNextDay'] == 1), :]  # departure must be before arrival or the trip must end the following day
    tripData = tripData.loc[(tripData['tripStartClock'] != ' ') & (tripData['tripEndClock'] != ' '), :]  # Timestamps must be strings
    tripData = tripData.loc[(tripData['tripDistance'] < 1000), :]  # 9994, 9999 and 70703 are values for implausible, missing or non-detailed values
    if dataset == 'MiD17':
        tripData = tripData.loc[tripData['tripIsIntermodal'] != 1, :]  # no intermodal trips
    if dataset == 'MiD08':
        tripData = tripData.loc[~tripData['tripPurpose'].isin([97, 98]), :]  # observations with refused or unknown purpose are filtered out
    # tripData = filterOutTripsBelongingToMultiModalDays(tripData)
    print('Finished filtering')

    # FIXME Currently, trips starting before 12:00 and ending at 13:00 have an endhour share of 0 and a fullHour. Change this to the respective share for two-hour trips
    # Variable replacements
    tripData.loc[:, 'ST_WOTAG_str'] = replaceDayNumbersByStrings(tripData.loc[:, 'tripStartWeekday'])
    tripData.loc[:, 'zweck_str'] = replacePurposes(tripData.loc[:, 'tripPurpose'],
                                                   config['midReplacements']['tripPurpose'])
    tripData['indexCol'] = tripData['hhPersonID'].astype('string') + '__' + tripData['tripID'].astype('string')
    tripData.set_index('indexCol', inplace=True)
    tripDataWDate = assignTSToColViaCWeek(tripData, 'tripStartYear', 'tripStartWeek', 'tripStartWeekday',
                                          'tripStartHour', 'tripStartMinute', 'timestamp_st')
    tripDataWDate = assignTSToColViaCWeek(tripDataWDate, 'tripStartYear', 'tripStartWeek', 'tripStartWeekday',
                                          'tripEndHour', 'tripEndMinute', 'timestamp_en')
    tripDataNightTrips = updateEndTimestamp(tripDataWDate)
    print('Finished timeseries replacements')


    # FIXME Currently, trips starting before 12:00 and ending at 13:00 have an endhour share of 0 and a fullHour. Change this to the respective share for two-hour trips
    tripDataWHourlyShares = calcHourlyShares(tripDataNightTrips, ts_st='timestamp_st', ts_en='timestamp_en')

    # ToDo: Fix implausible trips where shareStartHour = NAN or a share with shareEndHour and noOfFullHours both =0
    tripDataClean = tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    emptyDF = initiateHourDataframe(tripDataClean.index, config['numberOfHours'])

    # Class instantiating of callable class
    fillHourValues = FillHourValues(data=tripDataClean, rangeFunction=initiateColRange)
    driveDataTrips = fillDataframe(emptyDF, fillFunction=fillHourValues)

    driveDataTrips.loc[:, ['hhPersonID', 'tripID']] = pd.DataFrame(tripDataClean.loc[:, ['hhPersonID', 'tripID']])
    driveDataDays = mergeTrips(driveDataTrips)  # FIXME: What happens if two trips overlap?

    tripPurposesDriving = assignDriving(driveDataDays)
#    tripDataDatetime = hoursToDatetime(tripDataClean)
    print('Finished hourly dataframe replacements')

    purposeDataDays = fillDayPurposes(tripDataClean, tripPurposesDriving)
    purposeDataDays.replace({'0.0': 'HOME'})  # FIXME: This was a quick-fix, integrate into case-differentiation
    print('Finished purpose replacements')
    print(f'There are {len(purposeDataDays)} daily trip diaries.')

    indexedDriveData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['ST_WOTAG_str'])  # 'tripWeight', 'tripScaleFactor'
    print(purposeDataDays.head())
    print(tripDataClean.head())
    indexedPurposeData = mergeVariables(data=purposeDataDays, variableData=tripDataClean, variables=['ST_WOTAG_str'])  # 'tripWeight', 'tripScaleFactor'

    print(indexedDriveData.head())
    print(indexedPurposeData.head())
    writeOut(config=config, dataset=dataset, dataDrive=indexedDriveData, dataPurpose=indexedPurposeData)



if __name__ == '__main__':
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    parseMiD(dataset='MiD17', config=config)
