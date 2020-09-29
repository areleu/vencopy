__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '08.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from scripts.utilsParsing import *
from scripts.libPlotting import *
from scripts.libLogging import logger
import pathlib

# ToDo: Hebefaktoren einrechnen bei nicht-detaillierter Erfassung (siehe MID-Nuterhandbuch, S. 33f.)

if __name__ == '__main__':
    #----- data and config read-in -----
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # hhData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017households'], sep=';')
    # personData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017persons'], sep=';')
    tripData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017trips'], sep=';', decimal=',')

    tripData = tripData_raw.loc[tripData_raw.loc[:, 'W_VM_G'] == 1, ['HP_ID_Reg', 'W_ID', 'W_GEW', 'W_HOCH', 'W_SZ',
                                                'W_AZ', 'zweck', 'wegkm', 'ST_JAHR', 'ST_MONAT', 'ST_WOCHE', 'ST_WOTAG',
                                                'W_SZS', 'W_SZM', 'W_AZS', 'W_AZM', 'W_FOLGETAG', 'weg_intermod']]

    # Dataset filtering
    tripData = tripData.loc[(tripData['W_SZS'] != 99) & (tripData['W_AZS'] != 99), :]
    tripData = tripData.loc[(tripData['W_SZS'] != 701) & (tripData['W_AZS'] != 701), :]
    tripData = tripData.loc[tripData['W_SZ'] <= tripData['W_AZ'], :]
    tripData = tripData.loc[(tripData['wegkm'] < 1000), :]  # 9994, 9999 and 70703 are values for implausible, missing or non-detailed values
    tripData = tripData.loc[tripData['weg_intermod'] != 1, :]

    tripData.loc[:, 'ST_WOTAG_str'] = replaceDayNumbersByStrings(tripData.loc[:, 'ST_WOTAG'])
    tripData['indexCol'] = tripData['HP_ID_Reg'].astype('string') + '__' + tripData['W_ID'].astype('string')
    tripData.set_index('indexCol', inplace=True)
    tripDataWDate = assignTSToColViaCWeek(tripData, 'ST_JAHR', 'ST_WOCHE', 'ST_WOTAG', 'W_SZS', 'W_SZM', 'timestamp_st')
    tripDataWDate = assignTSToColViaCWeek(tripDataWDate, 'ST_JAHR', 'ST_WOCHE', 'ST_WOTAG', 'W_AZS', 'W_AZM',
                                          'timestamp_en')
    tripDataNightTrips = updateEndTimestamp(tripDataWDate)

    tripDataWHourlyShares = calcHourlyShares(tripDataWDate, ts_st='timestamp_st', ts_en='timestamp_en')

    # ToDo: Fix implausible trips where shareStartHour = NAN or a share with shareEndHour and noOfFullHours both =0
    tripDataClean = tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    emptyDF = initiateHourDataframe(tripDataClean.index, 24)

    # Class instantiating of callable class
    fillHourValues = FillHourValues(data=tripDataClean, rangeFunction=initiateColRange)
    driveDataTrips = fillDataframe(emptyDF, fillFunction=fillHourValues)




    driveDataTrips.loc[:, ['HP_ID_Reg', 'W_ID']] = pd.DataFrame(tripDataClean.loc[:, ['HP_ID_Reg', 'W_ID']], dtype=int)
    driveDataDays = mergeTrips(driveDataTrips)
    tripPurposesDriving = assignDriving(driveDataDays)
    purposeDataDays = fillDayPurposes(tripDataClean, tripPurposesDriving)


    # fillTripPurposes = FillTripPurposes(tripData=tripDataClean,
    #                                     mergedDayTrips=driveDataDays,
    #                                     rangeFunction=initiateColRange)
    # tripPurposes = fillDataframe(tripPurposes, tripDataClean)

    indexedData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['ST_WOTAG_str', 'W_GEW', 'W_HOCH'])

    print(indexedData.head())
    indexedData.to_csv('./inputData/inputProfiles_Drive_MiD17.csv', na_rep=0)
