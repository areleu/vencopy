__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.04.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import time
from pathlib import Path

import numpy as np
import pandas as pd
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict, writeOut

IDXSLICE = pd.IndexSlice


class DiaryBuilder:
    def __init__(self, configDict: dict, datasetID: str, activities: pd.DataFrame, isWeekDiary: bool = False):
        self.diaryConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.isWeekDiary = isWeekDiary
        self.distributedActivities = TimeDiscretiser(
            datasetID=self.datasetID, globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig, activities=self.activities, dt=self.deltaTime, isWeek=isWeekDiary,
            method="distribute")
        self.selectedActivities = TimeDiscretiser(
            datasetID=self.datasetID, globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig, activities=self.activities, dt=self.deltaTime, isWeek=isWeekDiary,
            method="select")

    def createDiaries(self):
        # self.drain = self.distributedActivities.discretise(column="drain")
        # self.chargingPower = self.selectedActivities.discretise(column="chargingPower")
        self.uncontrolledCharge = self.distributedActivities.discretise(column="uncontrolledCharge")
        # self.maxBatteryLevel = self.selectedActivities.discretise(column="maxBatteryLevelStart")
        # self.minBatteryLevel = self.selectedActivities.discretise(column="minBatteryLevelStart")
        # # self.residualNeed = self.distributedActivities.discretise(column="residualNeed") # in elec terms kWh elec
        # # self.maxBatteryLevelEnd = self.selectedActivities.discretise(column="maxBatteryLevelEnd")
        # # self.minBatteryLevelEnd = self.selectedActivities.discretise(column="minBatteryLevelEnd")


class WeekDiaryBuilder:
    def __init__(self, activities: pd.DataFrame, catCols: list[str], seed: int = None):
        """
        Class that synthesizes weekly activity chains from daily activity chains. This is done for a specific set of
        categories that is determined by columns in the activities data set. The cross-product of the unique entries
        of each category column forms the number of categories for merging randomly drawn day activities within
        each set. The user can then give the number of sampled weeks within each sample base that should be
        synthesized from the day chains.

        :param activities: Activities dataset with daily activity chains. Has to contain all VencoPy internal column
        names.
        :param catCols: List of strings giving the column names that name the category dimensions as sample base.

        """
        self.activities = activities
        self.catCols = catCols
        self.weekdayIDs = list(range(1, 8))

        self.__days = None
        self.dayWeekMap = None
        self.weekActivities = None

        self.samplingCols = catCols.copy()
        self.samplingCols.append('tripStartWeekday')

        # Combinatorics of categories and weekdays
        self.categories = self.__retrieveUniqueCategories(acts=self.activities, catCols=self.catCols)
        self.categoryID = self.__createCategoryIndex(uCats=self.categories, catNames=self.catCols)
        self.sampleBaseID = self.__createSampleBases(cats=self.categories, samplingCols=self.samplingCols)

        # Merging uniqueIDs to activities - assigning day activity chains to sampling bases
        self.activities = self.__assignCategoryID(acts=self.activities, catIDs=self.categoryID,
                                                  catCols=self.catCols)
        self.activities = self.__assignSamplingBaseID(acts=self.activities, sampleBaseIDs=self.sampleBaseID)
        self.sampleBaseInAct = self.__subsetSampleBase()

        # Will be set in composeWeekActivities, called after instantiation
        self.sampleSize = None

    def __retrieveUniqueCategories(self, acts: pd.DataFrame, catCols: list[str]):
        return [set(acts.loc[:, c]) for c in catCols]

    def __createCategoryIndex(self, uCats: list, catNames: list[str]):
        mIdx = pd.MultiIndex.from_product(uCats, names=catNames)
        return self.__dfFromMultiIndex(multiIndex=mIdx, colname='categoryID')

    def __createSampleBases(self, cats: list[set], samplingCols: list[str]):
        levels = cats.copy()
        levels.append(self.weekdayIDs)
        mIdx = pd.MultiIndex.from_product(levels, names=samplingCols)
        return self.__dfFromMultiIndex(multiIndex=mIdx, colname='sampleBaseID')

    def __dfFromMultiIndex(self, multiIndex: pd.MultiIndex, colname: str = 'value'):
        df = pd.DataFrame(index=multiIndex)
        df = df.reset_index()
        df[colname] = df.index
        return df.set_index(multiIndex.names)

    def __assignCategoryID(self, acts: pd.DataFrame, catIDs: pd.Series, catCols: list[str]):
        acts.set_index(list(catIDs.index.names), inplace=True)
        acts = acts.merge(catIDs, left_index=True, right_index=True, how='left')
        return acts.reset_index(catCols)

    def __assignSamplingBaseID(self, acts: pd.DataFrame, sampleBaseIDs: pd.Series):
        idxCols = list(sampleBaseIDs.index.names)  # List of index column names
        acts.set_index(idxCols, inplace=True)
        acts = acts.merge(sampleBaseIDs, left_index=True, right_index=True, how='left')
        return acts.reset_index(idxCols)

    def __subsetSampleBase(self):
        """Filter the sample base to only the category combinations that really exist in the data set.

        Returns:
            pd.Series: A subset of self.sampleBaseID representing only the sample bases for the categories that
            exist and are thus not empty.
        """
        return self.sampleBaseID.loc[self.sampleBaseID['sampleBaseID'].isin(self.activities['sampleBaseID'])]

    def summarizeSamplingBases(self):
        print(f'There are {len(self.categoryID)} category combinations of the categories {self.catCols}.')
        print(f'There are {len(self.sampleBaseID)} sample bases (each category for every weekday).')

        nSBInAct = self.sampleBaseID['sampleBaseID'].isin(self.activities['sampleBaseID']).sum()
        print(f'Of those sample bases, {nSBInAct} category combinations exist in the activities data set')

        self.__days = self.activities.groupby(by=['genericID']).first()
        nSamplingBase = self.__days.groupby(by=self.samplingCols).count()
        sampleBaseLength = nSamplingBase.iloc[:, 0]

        smallestSampleBase = sampleBaseLength.loc[sampleBaseLength == min(sampleBaseLength)]
        largestSampleBase = sampleBaseLength.loc[sampleBaseLength == max(sampleBaseLength)]

        print(f'The number of samples in each sample base ranges from {smallestSampleBase}')
        print(f'to {largestSampleBase}.')
        print(f'The average sample size is approximately {sampleBaseLength.mean().round()}.')
        print(f'The median sample size is approximately {sampleBaseLength.median().round()}.')

    def __assignWeeks(self, nWeeks: int, how: str = 'random', seed: int = None, replace: bool = False):
        """Interface function to generate nWeeks weeks from the specified sample base. Here, the mapping of the
        genericID to the respective weekID is generated

        Args:
            nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
            how (str): Different sampling methods. Currently only random is implemented
            seed (int): Random seed for reproducibility
            replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
            docstring of randomSample for details.
        """

        if how == 'random':
            sample = self.__randomSample(nWeeks=nWeeks, seed=seed, replace=replace)
        elif how not in ['weighted', 'stratified']:
            raise NotImplementedError(f'The requested method {how} is not implemented. Please select "random", '
                                      f'"weighted" or "stratified".')
        return sample

    def __randomSample(self, nWeeks: int, seed: int = None, replace: bool = False) -> pd.DataFrame:
        """ Pulls nWeeks samples from each sample base. Each sample represents one day and is identified by one
        genericID. The weekdays are already differentiated within the sample bases thus sampleBase 1 may represent MON
        while sampleBase 2 represents TUE. Per default only as many samples can be drawn from the sample base as there
        are days in the original data set (no bootstrapping, replace=False). This can be overwritten by replace=True.

        Args:
            nWeeks (int): Number of samples to be pulled out of each sampleBase
            seed (int, optional): Random seed for reproducibility. Defaults to None.
            replace (bool): If True, an infinite number of samples can be drawn from the sampleBase (German: Mit
            zurücklegen), if False only as many as there are days in the sampleBase (no bootstrapping, German: Ohne
            zurücklegen). Defaults to False.

        Returns:
            pd.DataFrame: A pd.DataFrame with the three columns sampleBaseID, genericID and weekID where weekID is
            a range object for each sampleBase, genericID are the samples.
        """

        # Set seed for reproducibiilty for debugging
        if seed:
            np.random.seed(seed=seed)

        sample = pd.DataFrame()

        for sbID in self.sampleBaseInAct['sampleBaseID']:
            sampleBase = self.activities.loc[self.activities['sampleBaseID'] == sbID, 'genericID'].unique()
            subSample = np.random.choice(sampleBase, replace=replace, size=nWeeks)
            df = pd.DataFrame.from_dict({'sampleBaseID': sbID,
                                         'genericID': subSample,
                                         'weekID': list(range(nWeeks))})
            sample = pd.concat([sample, df])
        return sample

    def composeWeekActivities(self, nWeeks: int = 10, seed: int = None, replace: bool = False):
        """Wrapper function to call function for sampling each person (day mobility) to a specific week in a
        specified category. activityID and genericID are adapted to cover the weekly pattern of the sampled mobility
        days within each week.

        Args:
            nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
            seed (int): Seed for random choice from the sampling bases for reproducibility
            replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
            docstring of randomSample for details.
        """

        print(f'Composing weeks for {nWeeks} choices from each sample base.')

        self.sampleSize = nWeeks
        self.dayWeekMap = self.__assignWeeks(nWeeks=nWeeks, seed=seed, replace=replace)
        weekActs = self.__merge(dayWeekMap=self.dayWeekMap, dayActs=self.activities, index_col='genericID')
        weekActs = self.__adjustGenericID(acts=weekActs)
        weekActs = self.__orderViaWeekday(acts=weekActs)
        weekActs = self.__adjustActID(acts=weekActs)

        self.weekActivities = weekActs
        return weekActs

    def __merge(self, dayWeekMap: pd.DataFrame, dayActs: pd.DataFrame, index_col: str) -> pd.DataFrame:
        """Utility function to merge two dataframes on a column, via more performant index merging. Indices will
        be reset before returning the merged DataFrame.

        Args:
            left (pd.DataFrame): Left DataFrame for merging
            right (pd.DataFrame): Right DataFrame for merging
            index_col (str): String that determines a column in both DataFrames

        Returns:
            pd.DataFrame: Merged DataFrame
        """

        dayWeekMapIdx = dayWeekMap.set_index(index_col)
        dayActsIdx = dayActs.set_index(index_col)
        merged = dayWeekMapIdx.merge(dayActsIdx, left_index=True, right_index=True)
        return merged.reset_index(index_col)

    def __adjustGenericID(self, acts: pd.DataFrame) -> pd.DataFrame:
        """Replaces the generic ID that characterized a specific person living in a specific household (MiD) or
        a specific vehicle (KID) and its respective daily travel patterns with a combination of 7 genericIDs -
        one for every day of the week determined by the categories given in catCols in the class instantiation.
        The genericID column does not have to be stored elsewhere since hhPersonID exists as a separate column.

        Args:
            acts (pd.DataFrame): Activity data set at least with the columns 'genericID' and 'weekID'
        """
        acts['dayGenericID'] = acts['genericID']
        # acts['genericID'] = (acts['categoryID'].apply(str) + acts['weekID'].apply(str)).apply(int)
        acts['genericID'] = (acts['categoryID'].apply(str) + acts['weekID'].apply(str))
        return acts

    def __orderViaWeekday(self, acts) -> pd.DataFrame:
        # FIXME: Some weekday activities are not in order
        return acts.sort_values(by=['genericID', 'tripStartWeekday', 'timestampStart'])

    def __adjustActID(self, acts: pd.DataFrame) -> pd.DataFrame:
        """Replaces the activityID with a weekly identifier so that activities count up all the way within a
        sampled week instead of just in a day. Day activity IDs will be stored for debugging in a separate column.

        Args:
            acts (pd.DataFrame): Activity data set.
        """

        acts = self.__mergeParkActs(acts=acts)
        acts = self.__reassignParkIDs(acts=acts)
        acts = self.__reassignTripIDs(acts=acts)
        acts = self.__reassignActIDs(acts=acts)

        print('Finished weekly activity ID chaining')
        return acts

    def __mergeParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """ In a week activity data set, merge the last parking of a previous day with the first parking of the next
        day to one activity spanning two days.

        Args:
            acts (pd.DataFrame): Activity data set, where the week is identified by the column genericID and the
            day via the column tripStartWeekday
        """
        # FIXME: Correct the end timestamp of remaining park activity after merge

        # Calculate shifted columns for merging last and first day park acts and updating lastAct col
        acts = self.__neglectFirstParkActs(acts=acts)  # only for weekdays TUE-SUN
        acts, nextVars = self.__addNextActVars(acts=acts,
                                               vars=['genericID', 'parkID', 'timestampStart', 'timestampEnd',
                                                     'tripPurpose'])
        acts = self.__adjustEndTimestamp(acts=acts)
        acts = self.__addONParkVariable(acts=acts)
        # OLD implementation was here acts = self.__neglectFirstParkActs(acts=acts)
        acts = self.__updateLastWeekActs(acts=acts)
        acts = self.__removeNextActVarCols(acts=acts, nextVars=nextVars)
        print('Finished last and first daily parking to one parking activity')
        return acts

    def __addNextActVars(self, acts: pd.DataFrame, vars: list[str]) -> tuple[pd.DataFrame, list[str]]:
        vars_next = [v + '_next' for v in vars]
        acts[vars_next] = acts[vars].shift(-1)
        return acts, vars_next

    def __adjustEndTimestamp(self, acts: pd.DataFrame) -> pd.DataFrame:
        acts.loc[self.__getLastParkActsWOSun(acts),
                 'timestampEnd'] = acts.loc[self.__getLastParkActsWOSun(acts),
                                            'timestampStart_next']
        return acts

    def __addONParkVariable(self, acts: pd.DataFrame) -> pd.DataFrame:
        """Adds a column for merged park activities. This is important later for the maximum charged energy at the
        ON parking activities because the timestamps do not have the same day. For the calculation of the length of
        the parking activity, and consequently the maximum electricity that can be charged, this has to be taken
        into account.

        Args:
            acts (pd.DataFrame): Activity data set

        Returns:
            pd.DataFrame: Activity data set with the added column 'isSyntheticONPark'
        """

        acts['isSyntheticONPark'] = False
        acts.loc[self.__getLastParkActsWOSun(acts), 'isSyntheticONPark'] = True
        return acts

    def __neglectFirstParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """Removes all first parking activities with two exceptions: First parking on Mondays will be kept since
        they form the first activities of the week (genericID). Also, first parking activities directly after trips
        that end exactly at 00:00 are kept.

        Args:
            acts (pd.DataFrame): Activities with daily first activities for every day activity chain

        Returns:
            pd.DataFrame: Activities with weekly first activities for every week activity chain
        """

        # Exclude the second edge case of previous trip activities ending exactly at 00:00. In that case the
        # previous activity is a trip not a park, so the identification goes via type checking of shifted parkID.
        acts['prevParkID'] = acts['parkID'].shift(1)
        idxToNeglect = (self.__getFirstParkActsWOMon(acts)) & ~(acts['prevParkID'].isna())

        # Set isFirstActivity to False for the above mentioned edge case
        idxSetFirstActFalse = (acts['isFirstActivity']) & ~(acts['tripStartWeekday'] == 1)
        acts.loc[idxSetFirstActFalse, 'isFirstActivity'] = False

        return acts.loc[~idxToNeglect, :].drop(columns=['prevParkID'])

    def __getFirstParkActsWOMon(self, acts: pd.DataFrame) -> pd.Series:
        """ Select all first park activities except the first activities of Mondays - those will be the first
        activities of the week and thus remain unchanged.

        Args:
            acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isFirstActivity' and
            'tripStartWeekday'

        Returns:
            pd.Series: A boolean Series which is True for all first activities except the ones on the first day of the
        week, i.e. Mondays.
        """

        return (~acts['parkID'].isna()) & (acts['isFirstActivity']) & ~(acts['tripStartWeekday'] == 1)

    def __getLastParkActsWOSun(self, acts) -> pd.Series:
        """ Select all last park activities except the last activities of Sundays - those will be the last
        activities of the week and thus remain unchanged.

        Args:
            acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isLastActivity' and
            'tripStartWeekday'

        Returns:
            pd.Series: A boolean Series which is True for all last activities except the ones on the first day of
            the week, i.e. Sundays.
        """

        return (~acts['parkID'].isna()) & (acts['isLastActivity']) & ~(acts['tripStartWeekday'] == 7)

    def __updateLastWeekActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """ Updates the column isLastActivity for a week diary after merging 7 day activity chains to one week
        activity chain.

        Args:
            acts (pd.DataFrame): Activity data set that has to at least have the columns 'isLastActivity',
            'genericID' and 'nextGenericID'. The last one should be available from before by
            acts['genericID'].shift(-1)

        Returns:
            pd.DataFrame: The activity data set with an ammended column 'isLastActivity' updated for the week chain
        """

        isLastWeekAct = (acts['isLastActivity']) & (acts['genericID'] != acts['genericID_next'])
        acts.loc[:, 'isLastActivity'] = isLastWeekAct
        return acts

    def __removeNextActVarCols(self, acts: pd.DataFrame, nextVars: list[str]) -> pd.DataFrame:
        return acts.drop(columns=nextVars)

    def __reassignParkIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """ Resets the activitiy IDs of the day park chain (increasing acts per Day) with continuously increasing
        actvity IDs per week identified by genericID. The day activity IDs will be stored in a separate column
        'dayActID'

        Args:
            acts (pd.DataFrame): The park activities of the activity data set at least with the columns
            'genericID' and 'actID'

        Returns:
            pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
            activity IDs.
        """

        parkIdx = ~acts['parkID'].isna()
        acts.loc[parkIdx, 'dayActID'] = acts['parkID']  # backupping day activity IDs
        acts.loc[parkIdx, 'parkID'] = acts.loc[parkIdx, ['genericID', 'parkID']].groupby(
            by='genericID').apply(lambda week: pd.Series(range(len(week)))).values
        return acts

    def __reassignTripIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """ Resets the activitiy IDs of the day trip chain (increasing acts per day) with continuously increasing
        actvity IDs per week identified by genericID. The day activity IDs will be stored in a separate column
        'dayActID'

        Args:
            acts (pd.DataFrame): The trip activities of the activity data set at least with the columns
            'genericID', 'tripID' and 'actID'

        Returns:
            pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
            activity IDs.
        """

        tripIdx = ~acts['tripID'].isna()
        acts.loc[tripIdx, 'dayActID'] = acts['tripID']

        acts.loc[tripIdx, 'tripID'] = acts.loc[tripIdx, ['genericID', 'parkID']].groupby(
            by='genericID').apply(lambda week: pd.Series(range(len(week)))).values
        return acts

    def __reassignActIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """Reassigns the column actID from tripID and parkID and updates next and prevActID accordingly

        Args:
            acts (pd.DataFrame): Activities data set with weekly tripIDs and parkIDs

        Returns:
            pd.DataFrame: Activities with updated column actID, prevActID and nextActID
        """

        # FIXME: Improve performance, currently only this func takes 43 seconds
        acts.loc[~acts['tripID'].isna(), 'actID'] = acts.loc[:, 'tripID']
        acts.loc[~acts['parkID'].isna(), 'actID'] = acts.loc[:, 'parkID']
        acts.loc[~acts['isLastActivity'], 'nextActID'] = acts.loc[:, 'actID'].shift(-1)
        acts.loc[~acts['isFirstActivity'], 'prevActID'] = acts.loc[:, 'actID'].shift(1)
        return acts


class TimeDiscretiser:
    def __init__(self, activities: pd.DataFrame, dt: int, datasetID: str, method: str, globalConfig: dict,
                 localPathConfig: dict, isWeek: bool = False):
        """
        Class for discretisation of activities to fixed temporal resolution. Act is
        a pandas Series with a unique ID in the index, ts is a pandas dataframe with two
        columns: timestampStart and timestampEnd, dt is a pandas TimeDelta object
        specifying the fixed resolution that the discretisation should output. Method
        specifies how the discretisation should be carried out. 'Distribute' assumes
        act provides a divisible variable (energy, distance etc.) and distributes this
        depending on the time share of the activity within the respective time interval.
        'Select' assumes an undivisible variable such as power is given and selects
        the values for the given timestamps. For now: If start or end timestamp of an
        activity exactly hits the middle of a time interval (dt/2), the value is allocated
        if its ending but not if its starting (value set to 0). For dt=30 min, a parking
        activity ending at 9:15 with a charging availability of 11 kW, 11 kW will be assigned
        to the last slot (9:00-9:30) whereas if it had started at 7:45, the slot (7:30-8:00)
        is set to 0 kW.
        The quantum is the shortest possible time interval for the discretiser, hard
        coded in the init and given as a pandas.TimeDelta. Thus if 1 minute is selected
        discretisation down to resolutions of seconds are not possible.

        Args:
            act (pd.dataFrame): _description_
            column (str): String specifying the column of the activities data set that should be discretized
            dt (pd.TimeDelta): _description_
            method (str): The discretisation method. Must be one of 'distribute' or 'select'.
        """
        self.activities = activities
        self.datasetID = datasetID
        self.method = method
        self.oneActivity = None  # rename --> FIXME in self.datasetCleanup()
        self.localPathConfig = localPathConfig
        self.globalConfig = globalConfig
        self.quantum = pd.Timedelta(value=1, unit='min')
        self.dt = dt  # e.g. 15 min
        self.isWeek = isWeek
        self.nTimeSlots = self._nSlotsPerInterval(interval=pd.Timedelta(value=self.dt, unit='min'))
        if isWeek:
            self.timeDelta = (pd.timedelta_range(start='00:00:00', end='168:00:00', freq=f'{self.dt}T'))
            self.weekdays = self.activities['weekdayStr'].unique()
        else:  # is Day
            self.timeDelta = (pd.timedelta_range(start='00:00:00', end='24:00:00', freq=f'{self.dt}T'))
        self.timeIndex = list(self.timeDelta)

    def _nSlotsPerInterval(self, interval: pd.Timedelta):
        # Check if interval is an integer multiple of quantum
        # the minimum resolution is 1 min, case for resolution below 1 min
        if interval.seconds/60 < self.quantum.seconds/60:
            raise(ValueError(f'The specified resolution is not a multiple of {self.quantum} minute, '
                             f'which is the minmum possible resolution'))
        # Check if an integer number of intervals fits into one day (15 min equals 96 intervals)
        quot = (interval.seconds/3600/24)
        quotDay = pd.Timedelta(value=24, unit='h') / interval
        if ((1/quot) % int(1/quot) == 0):  # or (quot % int(1) == 0):
            return quotDay
        else:
            raise(ValueError(f'The specified resolution does not fit into a day.'
                             f'There cannot be {quotDay} finite intervals in a day'))

    def _datasetCleanup(self):
        # timestamp start and end, unique ID, column to discretise - get read of additional columns
        necessaryColumns = ['tripID', 'timestampStart', 'timestampEnd', 'genericID',
                            'parkID', 'isFirstActivity', 'isLastActivity', 'timedelta',
                            'actID', 'nextActID', 'prevActID'] + [self.columnToDiscretise]
        if self.isWeek:
            necessaryColumns = necessaryColumns + ['weekdayStr']
        # FIXME: rename self.oneActivity to self.oneProfile
        self.oneActivity = self.activities[necessaryColumns].copy()
        self._correctDataset()

    def _correctDataset(self):
        self._correctValues()
        self._correctTimestamp()
        # FIXME: need of a recheck that timstampStart(t) == timestampEnd(t-1)?

    def _correctValues(self):
        if self.columnToDiscretise == 'drain':
            self.oneActivity['drain'] = self.oneActivity['drain'].fillna(0)
        elif self.columnToDiscretise == 'uncontrolledCharge':
            # remove all rows with tripID
            # self.oneActivity = self.oneActivity[self.oneActivity['uncontrolledCharge'].notna()]
            # instead of removing rows with tripID, assign 0 to rows with tripID
            self.oneActivity['uncontrolledCharge'] = self.oneActivity['uncontrolledCharge'].fillna(0)
        elif self.columnToDiscretise == 'residualNeed':
            # pad NaN with 0
            self.oneActivity['residualNeed'] = self.oneActivity['residualNeed'].fillna(0)
        return self.oneActivity

    def _correctTimestamp(self):
        self.oneActivity['timestampStartCorrected'] = self.oneActivity['timestampStart'].dt.round(f'{self.dt}min')
        self.oneActivity['timestampEndCorrected'] = self.oneActivity['timestampEnd'].dt.round(f'{self.dt}min')

    def _createDiscretisedStructure(self):
        """ Create an empty dataframe with columns each representing one timedelta (e.g. one 15-min slot). Scope can 
        currently be either day (nCol = 24*60 / dt) or week - determined be self.isWeek (nCol= 7 * 24 * 60 / dt). 
        self.timeIndex is set on instantiation.
        """
        if self.isWeek:
            # FIXME: make more generic to be able to model higher temporal resolution than 1h
            nHours = len(list(self.timeIndex))-1
            hPerDay = int(nHours / len(self.weekdays))
            hours = range(hPerDay)
            self.discreteData = pd.DataFrame(
                index=self.oneActivity.genericID.unique(), columns=pd.MultiIndex.from_product([self.weekdays, hours]))
        else:
            self.discreteData = pd.DataFrame(
                index=self.oneActivity.genericID.unique(), columns=range(len(list(self.timeIndex))-1))

    def _identifyBinShares(self):  # calculate value share
        self._calculateValueBinsAndQuanta()
        self._identifyBins()
        # wrapper for method:
        if self.method == 'distribute':
            self._valueDistribute()
        elif self.method == 'select':
            self._valueSelect()
        else:
            raise(ValueError(
                f'Specified method {self.method} is not implemented please specify "distribute" or "select".'))

    def _calculateValueBinsAndQuanta(self):
        """FIXME: Please describe logic here
            FIXME: Is self.oneActivity a frame with all activities or just the first one?
            FIXME: Since quanta are not calculated here, think about changing the func name
            Niklas's understanding so far:
            Calculates the multiple of dt of the activity duration and stores it to column nBins. E.g. a 2h-activity
            with a dt of 15 mins would have a 8 in the column. Seems like this is still precise, e.g. a 2h and 3 min
            activity would have a 8.2 in the column nBins. At this point, oneActivity seems to be still all activities,
            later it becomes the row representing one vehicle (represented by one genericID)
            --> FIXME in self.datasetCleanup()
        """
        self.oneActivity['activityDuration'] = (
            self.oneActivity['timestampEndCorrected'] - self.oneActivity['timestampStartCorrected'])
        # remove rows with activitsDuration = 0 which cause division by zero in nBins calculation
        # self.oneActivity = self.oneActivity[self.oneActivity['activityDuration'] != pd.Timedelta(value=0, unit='min')]
        self.oneActivity = self.oneActivity.drop(
            self.oneActivity[self.oneActivity.timestampStartCorrected == self.oneActivity.timestampEndCorrected].index)
        self.oneActivity['nBins'] = self.oneActivity['activityDuration'] / (pd.Timedelta(value=self.dt, unit='min'))
        # self.activities['nSlots'] = self.activities['delta'] / (pd.Timedelta(value=self.dt, unit='min'))
        # self.activities['nFullSlots'] = np.floor(self.activities['nSlots'])
        # self.activities['nPartialSlots'] = np.ceil((self.activities['nSlots'])-self.activities['nFullSlots'])
        # self.activities['nQuantaPerActivity'] = (
        #       self.activities['delta'] / np.timedelta64(1, 'm')) / (self.quantum.seconds/60)

    def _valueDistribute(self):
        # FIXME: add double check for edge case treatment for nBins == 0 (happens in uncontrolled Charge)
        self.oneActivity['valPerBin'] = self.oneActivity[self.columnToDiscretise] / self.oneActivity['nBins']
        # self.activities['valQuantum'] = (
        #           self.activities[self.columnToDiscretise] / self.activities['nQuantaPerActivity'])
        # self.activities['valFullSlot'] = (self.activities['valQuantum'] * ((
        #           pd.Timedelta(value=self.dt, unit='min')).seconds/60)).round(6)

    def _valueSelect(self):
        self.oneActivity['valPerBin'] = self.oneActivity[self.columnToDiscretise]
        # self.activities['valFullSlot'] = self.activities[self.columnToDiscretise]
        # self.activities['valLastSLot'] = self.activities[self.columnToDiscretise]

    def _identifyBins(self):
        self._identifyFirstBin()
        self._identifyLastBin()

    def _identifyFirstBin(self):
        """FIXME: Add docstring
        """
        # FIXME: Continue working for weekly profiles here

        self.oneActivity['timestampStartCorrected'] = self.oneActivity['timestampStartCorrected'].apply(
            lambda x: pd.to_datetime(str(x)))
        dayStart = self.oneActivity['timestampStartCorrected'].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.oneActivity['dailyTimeDeltaStart'] = self.oneActivity['timestampStartCorrected'] - dayStart
        self.oneActivity['startTimeFromMidnightSeconds'] = self.oneActivity['dailyTimeDeltaStart'].apply(
            lambda x: x.seconds)
        bins = pd.DataFrame({'index': self.timeDelta})
        bins.drop(bins.tail(1).index, inplace=True)  # remove last element, which is zero
        self.binFromMidnightSeconds = bins['index'].apply(lambda x: x.seconds)
        # self.activities['firstBin'] = self.activities['startTimeFromMidnightSeconds'].apply(
        # lambda x: np.where(x >= self.binFromMidnightSeconds)[0][-1])
        # more efficient below (edge case of value bigger than any bin, index will be -1)
        self.oneActivity['firstBin'] = self.oneActivity['startTimeFromMidnightSeconds'].apply(
            lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)

    def _identifyLastBin(self):
        """FIXME: Add docstring
        """
        dayEnd = self.oneActivity['timestampEndCorrected'].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.oneActivity['dailyTimeDeltaEnd'] = self.oneActivity['timestampEndCorrected'] - dayEnd
        # Option 1
        # activitiesLength = self.activities['timedelta'].apply(lambda x: x.seconds)
        # self.activities['endTimeFromMidnightSeconds'] =
        #   self.activities['startTimeFromMidnightSeconds'] + activitiesLength
        # Option 2
        # self.activities['endTimeFromMidnightSeconds'] = self.activities['dailyTimeDeltaEnd'].apply(
        #               lambda x: x.seconds)
        # self.activities['lastBin'] = self.activities['endTimeFromMidnightSeconds'].apply(
        #                 lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)
        # Option 3
        self.oneActivity['lastBin'] = self.oneActivity['firstBin'] + self.oneActivity['nBins'] - 1
        # FIXME: more elegant way for lastBin +1 if lastActivity = True -> +1 ?
        self.oneActivity.loc[self.oneActivity['isLastActivity'], 'lastBin'] = (
            self.oneActivity.loc[self.oneActivity['isLastActivity'], 'lastBin'] + 1)

    def _allocateBinShares(self):
        self._overlappingActivities()  # identify shared events in bin and handle them
        if self.isWeek:
            self._allocateWeek()
        else:
            self._allocate()
        self._checkBinValues()

    def _checkBinValues(self):
        # verify all bins get a value assigned
        # pad with 0
        pass

    # def _dropNoLengthEvents(self):
    # moved before nBins calculation
    #     self.oneActivity.drop(
    #         self.oneActivity[self.oneActivity.timestampStartCorrected
    #         == self.oneActivity.timestampEndCorrected].index)

    # FIXME: Implement this func, gets important especially for low resolutions (e.g. 1h)
    def _overlappingActivities(self):
        pass
        # stategy if time resolution high enough so that event becomes negligible (now in calculateValueBinsAndQuanta)
        # self._dropNoLengthEvents()
        # define other strategies to treat overlapping events here

    def _allocateWeek(self):
        """ Wrapper method for allocating respective values per bin to days within a week. Expects that the activities
        are formatted in a way that genericID represents a unique week ID. The function then loops over the 7 weekdays
        and calls _allocate for each day a total of 7 times. 

        """
        for d in self.weekdays:
            self._allocate(self.oneActivity.loc[self.oneActivity['weekdayStr'] == d, :],
                           weekday=d)

    def _allocate(self, acts, weekday: str = None):
        """ FIXME: Add docstring
        """

        # FIXME: Performance improvements by 1. vectorization, 2. not subsetting but concatenating in the end,
        # 3. more efficient treatment of weeks e.g. looping just via days
        for id in acts['genericID'].unique():
            vehicleSubset = acts[acts['genericID'] == id].reset_index(drop=True)
            for irow in range(len(vehicleSubset)):
                if self.isWeek:
                    # colIdx = (weekday,
                    #           IDXSLICE[(vehicleSubset.loc[irow, 'firstBin']):(vehicleSubset.loc[irow, 'lastBin'])]
                    #          )
                    day = self.discreteData.loc[id, weekday]
                    day.loc[(vehicleSubset.loc[irow, 'firstBin']):(
                        vehicleSubset.loc[irow, 'lastBin'])] = vehicleSubset.loc[irow, 'valPerBin']
                    # self.discreteData[id, weekday] = day  This doesnt work for some reason, BEN?
                    self.discreteData.loc[id, weekday] = day.values
                else:
                    self.discreteData.loc[id, (vehicleSubset.loc[irow, 'firstBin']):(
                        vehicleSubset.loc[irow, 'lastBin'])] = vehicleSubset.loc[irow, 'valPerBin']

        return self.discreteData

    def _writeOutput(self):
        writeOut(dataset=self.discreteData, outputFolder='diaryOutput', datasetID=self.datasetID,
                 fileKey=('outputDiaryBuilder'), manualLabel=str(self.columnToDiscretise),
                 localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def discretise(self, column: str):
        self.columnToDiscretise = column
        print(f"Starting to discretise {self.columnToDiscretise}.")
        self._datasetCleanup()
        self._createDiscretisedStructure()
        self._identifyBinShares()
        self._allocateBinShares()
        self._writeOutput()
        print(f"Discretisation finished for {self.columnToDiscretise}.")
        self.columnToDiscretise = None
        return self.discreteData


if __name__ == '__main__':

    startTime = time.time()
    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpFlex.activities)
    vpDiary.createDiaries()

    elapsedTime = time.time() - startTime
    print('Elapsed time:', elapsedTime)
