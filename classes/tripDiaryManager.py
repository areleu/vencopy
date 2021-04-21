__version__ = '0.0.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod

import time
import pprint
import pandas as pd
import warnings
from pathlib import Path
from profilehooks import profile
import yaml
from scripts.libInput import *
from scripts.utilsParsing import *
from classes.parseManager import DataParser


class TripDiaryBuilder:
    def __init__(self, config: dict, ParseData: DataParser, datasetID: str = 'MiD17'):
        self.config = config
        self.tripDataClean = None
        self.tripDistanceDiary = None
        self.tripPurposeDiary = None
        # self.calculateConsistentHourlyShares(data=ParseData.data)
        # ONLY FOR DEBUGGING PURPOSES
        self.calculateConsistentHourlyShares(data=ParseData.data.loc[0:2000, :])
        self.tripDistanceAllocation()
        # self.hhPersonMap = self.mapHHPIDToTripID(self.tripDataClean)
        self.tripPurposeAllocation()
        writeOut(config=config, datasetID=datasetID, dataDrive=self.tripDistanceDiary,
                 dataPurpose=self.tripPurposeDiary)

    def calculateConsistentHourlyShares(self, data):
        print('Calculating hourly shares')
        tripDataWHourlyShares = calcHourlyShares(data, ts_st='timestampStart', ts_en='timestampEnd')

        # Filter out implausible hourly share combinations
        self.tripDataClean = tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                       (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                       (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    def tripDistanceAllocation(self):
        print('Trip distance diary setup starting')
        self.formatDF = initiateHourDataframe(indexCol=self.tripDataClean.index, nHours=self.config['numberOfHours'])
        fillHourValues = FillHourValues(data=self.tripDataClean, rangeFunction=initiateColRange)
        driveDataTrips = fillDataframe(self.formatDF, fillFunction=fillHourValues)
        driveDataTrips.loc[:, ['hhPersonID', 'tripID']] = pd.DataFrame(self.tripDataClean.loc[:, ['hhPersonID',
                                                                                                  'tripID']])
        self.tripDistanceDiary = mergeTrips(driveDataTrips)
        print('Finished trip distance diary setup')

    def tripPurposeAllocation(self):
        print('Starting trip purpose diary setup')
        tripPurposesDriving = assignDriving(self.tripDistanceDiary)
        self.tripPurposeDiary = fillDayPurposes(self.tripDataClean, tripPurposesDriving)
        self.tripPurposeDiary.replace({'0.0': 'HOME'})  # Replace remaining non-allocated purposes with HOME
        print('Finished purpose replacements')
        print(f'There are {len(self.tripPurposeDiary)} daily trip diaries.')

    # improved purpose allocation approach
    def mapHHPIDToTripID(self, tripData):
        idCols = self.tripDataClean.loc[:, ['hhPersonID', 'tripID']]
        idCols.loc['nextTripID'] = idCols['tripID'].shift(-1, fill_value=0)
        tripDict = dict.fromkeys(set(idCols['hhPersonID']))
        for ihhpID in tripDict.keys():
            tripDict[ihhpID] = set(idCols.loc[idCols['hhPersonID'] == ihhpID, 'tripID'])
        return tripDict

if __name__ == '__main__':
    linkConfig = Path.cwd().parent / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    vpData = DataParser(datasetID='MiD08', config=config, loadEncrypted=False)
    vpDiary = TripDiaryBuilder(config=config, ParseData=vpData)