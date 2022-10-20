import sys
import numpy as np
import pandas as pd
from pathlib import Path
from itertools import product
from profilehooks import profile
# Needed to run in VSCode properties currently
sys.path.append('.')

from vencopy.scripts.globalFunctions import loadConfigDict, createOutputFolders
from vencopy.classes.evaluators import Evaluator
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.dataParsers import ParseMiD

__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    # datasetID = 'KiD'
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig',
                   'evaluatorConfig')
    basePath = Path(__file__).parent.parent
    configDict = loadConfigDict(configNames, basePath)
    createOutputFolders(configDict=configDict)

    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, loadEncrypted=False)
    vpData.process()

    # Trip distance and purpose diary compositions
    vpTripDiary = TripDiaryBuilder(datasetID=datasetID, configDict=configDict, ParseData=vpData, debug=True)

    # Grid model application
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, gridModel='simple')
    vpGrid.calcGrid()

    # Evaluate drive and trip purpose profile
    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.plotParkingAndPowers(vpGrid=vpGrid)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    vpEval.plotAggregates()

    # Estimate charging flexibility based on driving profiles and charge connection
    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID,
                           ParseData=vpData, hourShareDrive=vpTripDiary.tripHourShares)
    vpFlex.baseProfileCalculation(nIter=10)
    vpFlex.filter(fuelDriveTolerance=1)
    vpFlex.aggregate()
    # vpFlex.correct()
    # vpFlex.normalize()
    # vpFlex.writeOut()
    vpEval.plotProfiles(flexEstimator=vpFlex)

    class WeekDiaryBuilder:
        @profile(immediate=True)
        def __init__(self, dayDiary: pd.DataFrame, indexedData: pd.DataFrame, catCol: str,
                     build_genericID_map=False):
            """
            Class to synthesize weekly profiles from daily profiles. Profiles are put together for the same classes,
            e.g. male and female drivers (catCol='HP_SEX') such that the first Monday by a female in the data set is
            followed by the first Tuesday in the MID data set. Remaining days at the end of each category are filled up
            by the last available day diary in that category for that day. Category sets and respective id mappings have
            to be provided by indexedData. New IDs are formed by a range index for each category. The internal variable
            that you want to pull out is WeekDiaryBuilder.diaryWeekPad. This is a pandas.DataFrame with a MultiIndex
            on the index containing indexIDs and all unique set elements of the given WeekDiaryBuilder.catCol (category)
            and a MultiIndex in the columns specifying weekdays (1-7) and hours (0-23). Around those 168 values there
            are 2 days (48 values resp. hours) as padding added before and after duplicating the first and last days.

            :param dayDiary: The diary of survey days - currently these contain trips, rated powers or hourly shares.
            :param indexedData: Pandas DataFrame, containing data to pull an index, such as a household-person-ID from
            :param catCol: String of an internal vencopy variable (such as tripState) to order the week diaries by.
            :param build_genericID_map: Boolean value determining if a genericID map should be created. This has to
                be done only once in the workflow, because the week diaries all map to the same day-survey-weights.
            """
            self.catCol = catCol
            self.diaryIn = dayDiary
            self.indexData = indexedData[catCol]
            self.indexedWeights = indexedData['tripWeight']
            self.indexSet = set(self.indexData.unique())
            self.weekdays = list(range(1, 8))
            self.hours = list(range(24))
            self.column_index = self.set_column_index()
            self.diaryIndexed = self.addIndex(left=dayDiary, right=self.indexData)
            self.categories = self.diaryIndexed.index.get_level_values(catCol).unique()
            self.diaryStacked = self.diaryIndexed.stack().reset_index()
            self.instances = self.calculate_instances()
            self.diaryPiv = self.pivotData(self.diaryStacked)
            if build_genericID_map:
                self.diaryWeek, self.diaryWeekNA, self.genericIDMap, self.genericIDMapNA = self.buildWeekDiary(
                    dataIn=self.diaryPiv, build_genericID_map=build_genericID_map)
                self.diaryWeekPad = self.padWeekDiary(self.diaryWeek)
                self.diaryWeekPad.attrs['genericIDMap'] = self.genericIDMap
            else:
                self.diaryWeek, self.diaryWeekNA = self.buildWeekDiary(dataIn=self.diaryPiv)
                self.diaryWeekPad = self.padWeekDiary(self.diaryWeek)

        def set_column_index(self):
            return pd.DataFrame(list(product(self.weekdays, self.hours))).set_index([0, 1]).index.set_names(['weekday',
                                                                                                             'hour'])

        def addIndex(self, left: pd.DataFrame, right: pd.DataFrame):
            return left.merge(right=right, how='left', left_index=True,
                              right_index=True).set_index(self.catCol, append=True)

        def calculate_instances(self):
            """Generate the mapping of the unique values of the respective categories (e.g. the numbers 1 to 16 for
            the 16 federal states of Germany if self.catCol is set to 'tripState') in the dataset. self.catCol must
            match with a vencopy-internal variable name from the replacement dict values in parseConfig.
            Returns a multiindex object with the first level 'instanceID' containing a rangeIndex counting from 0 up to
            the highest number of weekdays in the respective category (e.g. 0-30 if there are 31 trips in Hesse). This
            range index is mapped to the respective category element.

            Returns:
                pandas.MultiIndex: MultiIndex containing a non-unique level 'instanceID' and a non-unique level named
                after the respective name of the category column (e.g. 'tripState').
            """

            colnames = ['tripStartWeekday', self.catCol]
            inst = self.diaryStacked[colnames].value_counts()
            series = inst.groupby(level=self.catCol).max().astype(int)

            # Create all combinations between each category and maximum per-category instances (maximum of number of
            # weekdays)
            df = pd.concat([pd.DataFrame(list(product(range(int(series[i] / 24)), [i]))) for i in series.index])
            return df.set_index([0, 1]).index.set_names(names=['instanceID', self.catCol])

        def pivotData(self, data):
            data = data.rename(columns={'tripStartWeekday': 'weekday', 'level_3': 'instanceID'})
            return data.pivot(index=['genericID', self.catCol], columns=['weekday', 'instanceID']).droplevel(0, axis=1)

        def buildWeekDiary(self, dataIn, build_genericID_map=False):
            # instance = pd.DataFrame(0, index=self.emptydf.index[0], columns=self.weekdays)
            # idx = pd.IndexSlice
            dataRaw = pd.DataFrame(np.nan, index=self.instances, columns=self.column_index)
            genericIDsRaw = pd.DataFrame(index=dataRaw.index,
                                         columns=dataRaw.columns.get_level_values('weekday').drop_duplicates())
            for iCat, iDay in product(self.categories, self.weekdays):
                getSection = dataIn.loc[dataIn.index.get_level_values(self.catCol) == iCat,
                                        dataIn.columns.get_level_values('weekday') == iDay].dropna(axis=0)

                getSectionNew = getSection.droplevel(level='genericID').reset_index()

                # Reindex from household person ID to instance ID
                getSectionNew.loc[:, 'instanceID'] = range(len(getSectionNew))
                getSectionNew = getSectionNew.set_index(['instanceID', self.catCol], drop=True)
                dataRaw.loc[getSectionNew.index, getSectionNew.columns] = getSectionNew

                if build_genericID_map:  # Store generic ID to weekday and category mapping
                    indexLevel = getSection.index.get_level_values('genericID')
                    genericIDsRaw.loc[getSectionNew.index, iDay] = indexLevel

            data = dataRaw.fillna(method='ffill', axis=0)
            genericIDs = genericIDsRaw.fillna(method='ffill', axis=0)
            return (data, dataRaw, genericIDs, genericIDsRaw) if build_genericID_map else (data, dataRaw)

        def padWeekDiary(self, diary):
            idx = pd.IndexSlice
            dfPadPre = diary.loc[idx[:], idx[1, :]]
            dfPadPre.columns = dfPadPre.columns.set_levels(levels=[0], level='weekday')
            dfPadPost = diary.loc[idx[:], idx[7, :]]
            dfPadPost.columns = dfPadPre.columns.set_levels(levels=[8], level='weekday')
            return pd.concat([dfPadPre, dfPadPre, diary, dfPadPost, dfPadPost], axis=1)

        def composeWeights():
            print('This is the end')

    WDB_drive = WeekDiaryBuilder(dayDiary=vpFlex.driveProfiles, indexedData=vpData.dataIndexed, catCol='tripState',
                                 build_genericID_map=True)
    WDB_plug = WeekDiaryBuilder(dayDiary=vpFlex.plugProfiles, indexedData=vpData.dataIndexed, catCol='tripState')
    WDB_hShares = WeekDiaryBuilder(dayDiary=vpTripDiary.tripHourShares, indexedData=vpData.dataIndexed,
                                   catCol='tripState')

    vpFlexWeek = FlexEstimator(configDict=configDict, datasetID=datasetID, ParseData=vpData,
                               driveProfilesIn=WDB_drive.diaryWeekPad, plugProfilesIn=WDB_plug.diaryWeekPad,
                               hourShareDrive=WDB_hShares.diaryWeekPad)

    vpFlexWeek.baseProfileCalculation()
    vpFlexWeek.filter(scope='week', fuelDriveTolerance=1)
    vpFlexWeek.aggregate(scope='week')

    vpFlexWeek.profilesSOCMaxCons.to_csv(basePath / 'snippets/out/socMax.csv')
    vpFlexWeek.profilesSOCMinCons.to_csv(basePath / 'snippets/out/socMin.csv')
    vpFlexWeek.electricPowerProfilesCons.to_csv(basePath / 'snippets/out/drain.csv')
    vpFlexWeek.chargeProfilesUncontrolledCons.to_csv(basePath / 'snippets/out/uncontrolledCharging.csv')
    vpFlexWeek.plugProfilesCons.to_csv(basePath / 'snippets/out/plug.csv')
    vpFlexWeek.auxFuelDemandProfilesCons.to_csv(basePath / 'snippets/out/auxFuel.csv')
    # vpFlexWeek.correct()
    # vpFlex.normalize()
    # vpFlex.writeOut()
    # print(f'Total absolute electricity charged in uncontrolled charging: '
    #       f'{vpFlex.chargeProfilesUncontrolled.sum().sum()} based on MiD17')
