__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "01.07.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path

import time
import numpy as np
import pandas as pd
from typing import Optional
from vencopy.utils.globalFunctions import createFileName, writeOut


class DiaryBuilder:
    def __init__(
        self, configDict: dict, activities: pd.DataFrame, isWeekDiary: bool = False
    ):
        self.diaryConfig = configDict["diaryConfig"]
        self.globalConfig = configDict["globalConfig"]
        self.localPathConfig = configDict["localPathConfig"]
        self.datasetID = configDict["globalConfig"]["dataset"]
        self.activities = activities
        self.deltaTime = configDict["diaryConfig"]["TimeDelta"]
        self.isWeekDiary = isWeekDiary
        self._updateActivities()
        self.distributedActivities = TimeDiscretiser(
            datasetID=self.datasetID,
            globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig,
            activities=self.activities,
            dt=self.deltaTime,
            isWeek=isWeekDiary,
            method="distribute",
        )
        self.selectedActivities = TimeDiscretiser(
            datasetID=self.datasetID,
            globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig,
            activities=self.activities,
            dt=self.deltaTime,
            isWeek=isWeekDiary,
            method="select",
        )

    def _updateActivities(self):
        """
        Updates timestamps and removes activities whose length equals zero to avoid inconsistencies in profiles
        which are separatly discretised (interdependence at single vehicle level of drain, charging power etc i.e.
        no charging available when driving).
        """
        self._correctTimestamp()
        self._dropNoLengthEvents()

    def _correctTimestamp(self):
        """
        Rounds timestamps to predifined resolution.
        """
        self.activities["timestampStartCorrected"] = self.activities[
            "timestampStart"
        ].dt.round(f"{self.deltaTime}min")
        self.activities["timestampEndCorrected"] = self.activities[
            "timestampEnd"
        ].dt.round(f"{self.deltaTime}min")
        self.activities["activityDuration"] = (
            self.activities["timestampEndCorrected"] - self.activities["timestampStartCorrected"]
        )
        return self.activities

    def _dropNoLengthEvents(self):
        """
        Drops line when activity duration is zero, which causes inconsistencies in diaryBuilder (e.g. division by zero in nBins calculation).
        """
        startLength = len(self.activities)
        self.activities = self.activities.drop(
            self.activities[self.activities.activityDuration == pd.Timedelta(0)].index.to_list())
        endLength = len(self.activities)
        print(f"{startLength - endLength} activities dropped because activity length equals zero.")

    def createDiaries(self):
        start_time = time.time()
        self.drain = self.distributedActivities.discretise(column="drain")
        self.chargingPower = self.selectedActivities.discretise(column="availablePower")
        self.uncontrolledCharge = self.distributedActivities.discretise(
            column="uncontrolledCharge"
        )
        self.maxBatteryLevel = self.selectedActivities.discretise(
            column="maxBatteryLevelStart"
        )
        self.minBatteryLevel = self.selectedActivities.discretise(
            column="minBatteryLevelStart"
        )
        # # self.residualNeed = self.distributedActivities.discretise(column="residualNeed") # in elec terms kWh elec
        # # self.maxBatteryLevelEnd = self.selectedActivities.discretise(column="maxBatteryLevelEnd")
        # # self.minBatteryLevelEnd = self.selectedActivities.discretise(column="minBatteryLevelEnd")
        needed_time = time.time() - start_time
        print(f"Needed time to discretise all columns: {needed_time}")


class WeekDiaryBuilder:
    def __init__(
        self, activities: pd.DataFrame, catCols: list[str], seed: Optional[int] = None
    ):
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
        self.samplingCols.append("tripStartWeekday")

        # Combinatorics of categories and weekdays
        self.categories = self.__retrieveUniqueCategories(
            acts=self.activities, catCols=self.catCols
        )
        self.categoryID = self.__createCategoryIndex(
            uCats=self.categories, catNames=self.catCols
        )
        self.sampleBaseID = self.__createSampleBases(
            cats=self.categories, samplingCols=self.samplingCols
        )

        # Merging uniqueIDs to activities - assigning day activity chains to sampling bases
        self.activities = self.__assignCategoryID(
            acts=self.activities, catIDs=self.categoryID, catCols=self.catCols
        )
        self.activities = self.__assignSamplingBaseID(
            acts=self.activities, sampleBaseIDs=self.sampleBaseID
        )
        self.sampleBaseInAct = self.__subsetSampleBase()

        # Will be set in composeWeekActivities, called after instantiation
        self.sampleSize = None

    def __retrieveUniqueCategories(self, acts: pd.DataFrame, catCols: list[str]):
        return [set(acts.loc[:, c]) for c in catCols]

    def __createCategoryIndex(self, uCats: list, catNames: list[str]):
        mIdx = pd.MultiIndex.from_product(uCats, names=catNames)
        return self.__dfFromMultiIndex(multiIndex=mIdx, colname="categoryID")

    def __createSampleBases(self, cats: list[set], samplingCols: list[str]):
        levels = cats.copy()
        levels.append(self.weekdayIDs)
        mIdx = pd.MultiIndex.from_product(levels, names=samplingCols)
        return self.__dfFromMultiIndex(multiIndex=mIdx, colname="sampleBaseID")

    def __dfFromMultiIndex(self, multiIndex: pd.MultiIndex, colname: str = "value"):
        df = pd.DataFrame(index=multiIndex)
        df = df.reset_index()
        df[colname] = df.index
        return df.set_index(multiIndex.names)

    def __assignCategoryID(
        self, acts: pd.DataFrame, catIDs: pd.Series, catCols: list[str]
    ):
        acts.set_index(list(catIDs.index.names), inplace=True)
        acts = acts.merge(catIDs, left_index=True, right_index=True, how="left")
        return acts.reset_index(catCols)

    def __assignSamplingBaseID(self, acts: pd.DataFrame, sampleBaseIDs: pd.Series):
        idxCols = list(sampleBaseIDs.index.names)  # List of index column names
        acts.set_index(idxCols, inplace=True)
        acts = acts.merge(sampleBaseIDs, left_index=True, right_index=True, how="left")
        return acts.reset_index(idxCols)

    def __subsetSampleBase(self):
        """
        Filter the sample base to only the category combinations that really exist in the data set.

        Returns:
            pd.Series: A subset of self.sampleBaseID representing only the sample bases for the categories that
            exist and are thus not empty.
        """
        return self.sampleBaseID.loc[
            self.sampleBaseID["sampleBaseID"].isin(self.activities["sampleBaseID"])
        ]

    def summarizeSamplingBases(self):
        print(
            f"There are {len(self.categoryID)} category combinations of the categories {self.catCols}."
        )
        print(
            f"There are {len(self.sampleBaseID)} sample bases (each category for every weekday)."
        )
        nSBInAct = (
            self.sampleBaseID["sampleBaseID"]
            .isin(self.activities["sampleBaseID"])
            .sum()
        )
        print(
            f"Of those sample bases, {nSBInAct} category combinations exist in the activities data set"
        )
        self.__days = self.activities.groupby(by=["uniqueID"]).first()
        nSamplingBase = self.__days.groupby(by=self.samplingCols).count()
        sampleBaseLength = nSamplingBase.iloc[:, 0]
        smallestSampleBase = sampleBaseLength.loc[
            sampleBaseLength == min(sampleBaseLength)
        ]
        largestSampleBase = sampleBaseLength.loc[
            sampleBaseLength == max(sampleBaseLength)
        ]
        print(
            f"The number of samples in each sample base ranges from {smallestSampleBase}"
        )
        print(f"to {largestSampleBase}.")
        print(
            f"The average sample size is approximately {sampleBaseLength.mean().round()}."
        )
        print(
            f"The median sample size is approximately {sampleBaseLength.median().round()}."
        )

    def __assignWeeks(
        self,
        nWeeks: int,
        how: str = "random",
        seed: Optional[int] = None,
        replace: bool = False,
    ):
        """
        Interface function to generate nWeeks weeks from the specified sample base. Here, the mapping of the
        uniqueID to the respective weekID is generated

        Args:
            nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
            how (str): Different sampling methods. Currently only random is implemented
            seed (int): Random seed for reproducibility
            replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
            docstring of randomSample for details.
        """
        if how == "random":
            sample = self.__randomSample(nWeeks=nWeeks, seed=seed, replace=replace)
        elif how not in ["weighted", "stratified"]:
            raise NotImplementedError(
                f'The requested method {how} is not implemented. Please select "random", '
                f'"weighted" or "stratified".'
            )
        return sample

    def __randomSample(
        self, nWeeks: int, seed: Optional[int] = None, replace: bool = False
    ) -> pd.DataFrame:
        """
        Pulls nWeeks samples from each sample base. Each sample represents one day and is identified by one
        uniqueID. The weekdays are already differentiated within the sample bases thus sampleBase 1 may represent MON
        while sampleBase 2 represents TUE. Per default only as many samples can be drawn from the sample base as there
        are days in the original data set (no bootstrapping, replace=False). This can be overwritten by replace=True.

        Args:
            nWeeks (int): Number of samples to be pulled out of each sampleBase
            seed (int, optional): Random seed for reproducibility. Defaults to None.
            replace (bool): If True, an infinite number of samples can be drawn from the sampleBase (German: Mit
            zurücklegen), if False only as many as there are days in the sampleBase (no bootstrapping, German: Ohne
            zurücklegen). Defaults to False.

        Returns:
            pd.DataFrame: A pd.DataFrame with the three columns sampleBaseID, uniqueID and weekID where weekID is
            a range object for each sampleBase, uniqueID are the samples.
        """
        # Set seed for reproducibiilty for debugging
        if seed:
            np.random.seed(seed=seed)
        sample = pd.DataFrame()
        for sbID in self.sampleBaseInAct["sampleBaseID"]:
            sampleBase = self.activities.loc[
                self.activities["sampleBaseID"] == sbID, "uniqueID"
            ].unique()
            subSample = np.random.choice(sampleBase, replace=replace, size=nWeeks)
            df = pd.DataFrame.from_dict(
                {
                    "sampleBaseID": sbID,
                    "uniqueID": subSample,
                    "weekID": list(range(nWeeks)),
                }
            )
            sample = pd.concat([sample, df])
        return sample

    def composeWeekActivities(
        self, nWeeks: int = 10, seed: Optional[int] = None, replace: bool = False
    ):
        """
        Wrapper function to call function for sampling each person (day mobility) to a specific week in a
        specified category. activityID and uniqueID are adapted to cover the weekly pattern of the sampled mobility
        days within each week.

        Args:
            nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
            seed (int): Seed for random choice from the sampling bases for reproducibility
            replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
            docstring of randomSample for details.
        """
        print(f"Composing weeks for {nWeeks} choices from each sample base.")
        self.sampleSize = nWeeks
        self.dayWeekMap = self.__assignWeeks(nWeeks=nWeeks, seed=seed, replace=replace)
        weekActs = self.__merge(
            dayWeekMap=self.dayWeekMap, dayActs=self.activities, index_col="uniqueID"
        )
        weekActs = self.__adjustUniqueID(acts=weekActs)
        weekActs = self.__orderViaWeekday(acts=weekActs)
        weekActs = self.__adjustActID(acts=weekActs)
        self.weekActivities = weekActs
        return weekActs

    def __merge(
        self, dayWeekMap: pd.DataFrame, dayActs: pd.DataFrame, index_col: str
    ) -> pd.DataFrame:
        """
        Utility function to merge two dataframes on a column, via more performant index merging. Indices will
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

    def __adjustUniqueID(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces the generic ID that characterized a specific person living in a specific household (MiD) or
        a specific vehicle (KID) and its respective daily travel patterns with a combination of 7 uniqueIDs -
        one for every day of the week determined by the categories given in catCols in the class instantiation.
        The uniqueID column does not have to be stored elsewhere since hhPersonID exists as a separate column.

        Args:
            acts (pd.DataFrame): Activity data set at least with the columns 'uniqueID' and 'weekID'
        """
        acts["dayUniqueID"] = acts["uniqueID"]
        # acts['uniqueID'] = (acts['categoryID'].apply(str) + acts['weekID'].apply(str)).apply(int)
        acts["uniqueID"] = acts["categoryID"].apply(str) + acts["weekID"].apply(str)
        return acts

    def __orderViaWeekday(self, acts) -> pd.DataFrame:
        return acts.sort_values(by=["uniqueID", "tripStartWeekday", "timestampStart"])

    def __adjustActID(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces the activityID with a weekly identifier so that activities count up all the way within a
        sampled week instead of just in a day. Day activity IDs will be stored for debugging in a separate column.

        Args:
            acts (pd.DataFrame): Activity data set.
        """
        acts = self.__mergeParkActs(acts=acts)
        acts = self.__reassignParkIDs(acts=acts)
        acts = self.__reassignTripIDs(acts=acts)
        acts = self.__reassignActIDs(acts=acts)

        print("Finished weekly activity ID chaining")
        return acts

    def __mergeParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        In a week activity data set, merge the last parking of a previous day with the first parking of the next
        day to one activity spanning two days.

        Args:
            acts (pd.DataFrame): Activity data set, where the week is identified by the column uniqueID and the
            day via the column tripStartWeekday
        """
        # Calculate shifted columns for merging last and first day park acts and updating lastAct col
        acts = self.__neglectFirstParkActs(acts=acts)  # only for weekdays TUE-SUN
        acts, nextVars = self.__addNextActVars(
            acts=acts,
            vars=[
                "uniqueID",
                "parkID",
                "timestampStart",
                "timestampEnd",
                "tripPurpose",
            ],
        )
        acts = self.__adjustEndTimestamp(acts=acts)
        acts = self.__addONParkVariable(acts=acts)
        # OLD implementation was here acts = self.__neglectFirstParkActs(acts=acts)
        acts = self.__updateLastWeekActs(acts=acts)
        acts = self.__removeNextActVarCols(acts=acts, nextVars=nextVars)
        print("Finished last and first daily parking to one parking activity")
        return acts

    def __addNextActVars(
        self, acts: pd.DataFrame, vars: list[str]
    ) -> tuple[pd.DataFrame, list[str]]:
        vars_next = [v + "_next" for v in vars]
        acts[vars_next] = acts[vars].shift(-1)
        return acts, vars_next

    def __adjustEndTimestamp(self, acts: pd.DataFrame) -> pd.DataFrame:
        acts.loc[self.__getLastParkActsWOSun(acts), "timestampEnd"] = acts.loc[
            self.__getLastParkActsWOSun(acts), "timestampStart_next"
        ]
        return acts

    def __addONParkVariable(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a column for merged park activities. This is important later for the maximum charged energy at the
        ON parking activities because the timestamps do not have the same day. For the calculation of the length of
        the parking activity, and consequently the maximum electricity that can be charged, this has to be taken
        into account.

        Args:
            acts (pd.DataFrame): Activity data set

        Returns:
            pd.DataFrame: Activity data set with the added column 'isSyntheticONPark'
        """
        acts["isSyntheticONPark"] = False
        acts.loc[self.__getLastParkActsWOSun(acts), "isSyntheticONPark"] = True
        return acts

    def __neglectFirstParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Removes all first parking activities with two exceptions: First parking on Mondays will be kept since
        they form the first activities of the week (uniqueID). Also, first parking activities directly after trips
        that end exactly at 00:00 are kept.

        Args:
            acts (pd.DataFrame): Activities with daily first activities for every day activity chain

        Returns:
            pd.DataFrame: Activities with weekly first activities for every week activity chain
        """
        # Exclude the second edge case of previous trip activities ending exactly at 00:00. In that case the
        # previous activity is a trip not a park, so the identification goes via type checking of shifted parkID.
        acts["prevParkID"] = acts["parkID"].shift(1)
        idxToNeglect = (self.__getFirstParkActsWOMon(acts)) & ~(
            acts["prevParkID"].isna()
        )

        # Set isFirstActivity to False for the above mentioned edge case
        idxSetFirstActFalse = (acts["isFirstActivity"]) & ~(
            acts["tripStartWeekday"] == 1
        )
        acts.loc[idxSetFirstActFalse, "isFirstActivity"] = False
        return acts.loc[~idxToNeglect, :].drop(columns=["prevParkID"])

    def __getFirstParkActsWOMon(self, acts: pd.DataFrame) -> pd.Series:
        """
        Select all first park activities except the first activities of Mondays - those will be the first
        activities of the week and thus remain unchanged.

        Args:
            acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isFirstActivity' and
            'tripStartWeekday'

        Returns:
            pd.Series: A boolean Series which is True for all first activities except the ones on the first day of the
        week, i.e. Mondays.
        """
        return (
            (~acts["parkID"].isna())
            & (acts["isFirstActivity"])
            & ~(acts["tripStartWeekday"] == 1)
        )

    def __getLastParkActsWOSun(self, acts) -> pd.Series:
        """
        Select all last park activities except the last activities of Sundays - those will be the last
        activities of the week and thus remain unchanged.

        Args:
            acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isLastActivity' and
            'tripStartWeekday'

        Returns:
            pd.Series: A boolean Series which is True for all last activities except the ones on the first day of
            the week, i.e. Sundays.
        """
        return (
            (~acts["parkID"].isna())
            & (acts["isLastActivity"])
            & ~(acts["tripStartWeekday"] == 7)
        )

    def __updateLastWeekActs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Updates the column isLastActivity for a week diary after merging 7 day activity chains to one week
        activity chain.

        Args:
            acts (pd.DataFrame): Activity data set that has to at least have the columns 'isLastActivity',
            'uniqueID' and 'nextUniqueID'. The last one should be available from before by
            acts['uniqueID'].shift(-1)

        Returns:
            pd.DataFrame: The activity data set with an ammended column 'isLastActivity' updated for the week chain
        """

        isLastWeekAct = (acts["isLastActivity"]) & (
            acts["uniqueID"] != acts["uniqueID_next"]
        )
        acts.loc[:, "isLastActivity"] = isLastWeekAct
        return acts

    def __removeNextActVarCols(
        self, acts: pd.DataFrame, nextVars: list[str]
    ) -> pd.DataFrame:
        return acts.drop(columns=nextVars)

    def __reassignParkIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Resets the activitiy IDs of the day park chain (increasing acts per Day) with continuously increasing
        actvity IDs per week identified by uniqueID. The day activity IDs will be stored in a separate column
        'dayActID'

        Args:
            acts (pd.DataFrame): The park activities of the activity data set at least with the columns
            'uniqueID' and 'actID'

        Returns:
            pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
            activity IDs.
        """
        parkIdx = ~acts["parkID"].isna()
        acts.loc[parkIdx, "dayActID"] = acts["parkID"]  # backupping day activity IDs
        acts.loc[parkIdx, "parkID"] = (
            acts.loc[parkIdx, ["uniqueID", "parkID"]]
            .groupby(by="uniqueID")
            .apply(lambda week: pd.Series(range(len(week))))
            .values
        )
        return acts

    def __reassignTripIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Resets the activitiy IDs of the day trip chain (increasing acts per day) with continuously increasing
        actvity IDs per week identified by uniqueID. The day activity IDs will be stored in a separate column
        'dayActID'
        Args:
            acts (pd.DataFrame): The trip activities of the activity data set at least with the columns
            'uniqueID', 'tripID' and 'actID'
        Returns:
            pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
            activity IDs.
        """
        tripIdx = ~acts["tripID"].isna()
        acts.loc[tripIdx, "dayActID"] = acts["tripID"]

        acts.loc[tripIdx, "tripID"] = (
            acts.loc[tripIdx, ["uniqueID", "parkID"]]
            .groupby(by="uniqueID")
            .apply(lambda week: pd.Series(range(len(week))))
            .values
        )
        return acts

    def __reassignActIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
        """
        Reassigns the column actID from tripID and parkID and updates next and prevActID accordingly
        Args:
            acts (pd.DataFrame): Activities data set with weekly tripIDs and parkIDs
        Returns:
            pd.DataFrame: Activities with updated column actID, prevActID and nextActID
        """
        acts.loc[~acts["tripID"].isna(), "actID"] = acts.loc[:, "tripID"]
        acts.loc[~acts["parkID"].isna(), "actID"] = acts.loc[:, "parkID"]
        acts.loc[~acts["isLastActivity"], "nextActID"] = acts.loc[:, "actID"].shift(-1)
        acts.loc[~acts["isFirstActivity"], "prevActID"] = acts.loc[:, "actID"].shift(1)
        return acts


class TimeDiscretiser:
    def __init__(
        self,
        activities: pd.DataFrame,
        dt: int,
        datasetID: str,
        method: str,
        globalConfig: dict,
        localPathConfig: dict,
        isWeek: bool = False,
    ):
        """
        Class for discretisation of activities to fixed temporal resolution

        Activities is a pandas Series with a unique ID in the index, ts is a pandas dataframe with two
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
        self.dataToDiscretise = None
        self.localPathConfig = localPathConfig
        self.globalConfig = globalConfig
        self.quantum = pd.Timedelta(value=1, unit="min")
        self.dt = dt  # e.g. 15 min
        self.isWeek = isWeek
        self.nTimeSlots = int(
            self._nSlotsPerInterval(interval=pd.Timedelta(value=self.dt, unit="min"))
        )
        if isWeek:
            self.timeDelta = pd.timedelta_range(
                start="00:00:00", end="168:00:00", freq=f"{self.dt}T"
            )
            self.weekdays = self.activities["weekdayStr"].unique()
        else:  # is Day
            self.timeDelta = pd.timedelta_range(
                start="00:00:00", end="24:00:00", freq=f"{self.dt}T"
            )
        self.timeIndex = list(self.timeDelta)
        self.discreteData = None

    def _nSlotsPerInterval(self, interval: pd.Timedelta):
        """
        Check if interval is an integer multiple of quantum.
        The minimum resolution is 1 min, case for resolution below 1 min.
        Then check if an integer number of intervals fits into one day (15 min equals 96 intervals)
        """
        if interval.seconds / 60 < self.quantum.seconds / 60:
            raise (
                ValueError(
                    f"The specified resolution is not a multiple of {self.quantum} minute, "
                    f"which is the minmum possible resolution"
                )
            )
        quot = interval.seconds / 3600 / 24
        quotDay = pd.Timedelta(value=24, unit="h") / interval
        if (1 / quot) % int(1 / quot) == 0:  # or (quot % int(1) == 0):
            return quotDay
        else:
            raise (
                ValueError(
                    f"The specified resolution does not fit into a day."
                    f"There cannot be {quotDay} finite intervals in a day"
                )
            )

    def _datasetCleanup(self):
        self._removeColumns()
        self._correctValues()
        self._correctTimestamp()

    def _removeColumns(self):
        """
        Removes additional columns not used in the TimeDiscretiser class.
        Only keeps timestamp start and end, unique ID, and the column to discretise.
        """
        necessaryColumns = [
            "tripID",
            "timestampStart",
            "timestampEnd",
            "uniqueID",
            "parkID",
            "isFirstActivity",
            "isLastActivity",
            "timedelta",
            "actID",
            "nextActID",
            "prevActID",
        ] + [self.columnToDiscretise]
        if self.isWeek:
            necessaryColumns = necessaryColumns + ["weekdayStr"]
        self.dataToDiscretise = self.activities[necessaryColumns].copy()
        return self.dataToDiscretise

    def _correctValues(self):
        """
        Depending on the columns to discretise correct some values.
        - drain profile: pads NaN with 0s
        - uncontrolledCharge profile: instead of removing rows with tripID, assign 0 to rows with tripID
        - residualNeed profile: pads NaN with 0s
        """
        if self.columnToDiscretise == "drain":
            self.dataToDiscretise["drain"] = self.dataToDiscretise["drain"].fillna(0)
        elif self.columnToDiscretise == "uncontrolledCharge":
            self.dataToDiscretise["uncontrolledCharge"] = self.dataToDiscretise[
                "uncontrolledCharge"
            ].fillna(0)
        elif self.columnToDiscretise == "residualNeed":
            self.dataToDiscretise["residualNeed"] = self.dataToDiscretise["residualNeed"].fillna(0)
        return self.dataToDiscretise

    def _correctTimestamp(self):
        """
        Rounds timestamps to predifined resolution.
        """
        self.dataToDiscretise["timestampStartCorrected"] = self.dataToDiscretise[
            "timestampStart"
        ].dt.round(f"{self.dt}min")
        self.dataToDiscretise["timestampEndCorrected"] = self.dataToDiscretise[
            "timestampEnd"
        ].dt.round(f"{self.dt}min")
        return self.dataToDiscretise

    def _createDiscretisedStructureWeek(self):
        """
        Create an empty dataframe with columns each representing one timedelta (e.g. one 15-min slot). Scope can
        currently be either day (nCol = 24*60 / dt) or week - determined be self.isWeek (nCol= 7 * 24 * 60 / dt).
        self.timeIndex is set on instantiation.
        """
        nHours = len(list(self.timeIndex)) - 1
        hPerDay = int(nHours / len(self.weekdays))
        hours = range(hPerDay)
        self.discreteData = pd.DataFrame(
            index=self.dataToDiscretise.uniqueID.unique(),
            columns=pd.MultiIndex.from_product([self.weekdays, hours]),
        )
        self.discreteDataFast = (
            self.discreteData.copy()
        )  # Only for performance analysis

    def _identifyBinShares(self):
        """
        Calculate value share to be assigned to bins and identifies the bins.
        Includes a wrapper for the 'distribute' und 'select' method.
        """
        self._calculateValueBins()
        self._identifyBins()
        # wrapper for method:
        if self.method == "distribute":
            self._valueDistribute()
        elif self.method == "select":
            self._valueSelect()
        else:
            raise (
                ValueError(
                    f'Specified method {self.method} is not implemented please specify "distribute" or "select".'
                )
            )

    def _calculateValueBins(self):
        """
        Updates the activity duration based on the rounded timstamps.
        Calculates the multiple of dt of the activity duration and stores it to column nBins. E.g. a 2h-activity
        with a dt of 15 mins would have a 8 in the column.
        """
        self.dataToDiscretise["activityDuration"] = (
            self.dataToDiscretise["timestampEndCorrected"]
            - self.dataToDiscretise["timestampStartCorrected"]
        )
        self._dropNoLengthEvents()
        self.dataToDiscretise["nBins"] = self.dataToDiscretise["activityDuration"] / (
            pd.Timedelta(value=self.dt, unit="min")
        )
        if not self.dataToDiscretise["nBins"].apply(float.is_integer).all():
            raise ValueError("Not all bin counts are integers.")
        self._dropNBinsLengthZero()

    def _dropNBinsLengthZero(self):
        """
        Drops line when nBins is zero, which cause division by zero in nBins calculation.
        """
        startLength = len(self.dataToDiscretise)
        self.dataToDiscretise.drop(
            self.dataToDiscretise[
                self.dataToDiscretise.nBins
                == 0
            ].index
        )
        endLength = len(self.dataToDiscretise)
        droppedProfiles = startLength - endLength
        print(f"{droppedProfiles} activities dropped because bin lenght equals zero.")

    def _valueDistribute(self):
        """
        Calculate the profile value for each bin for the 'distribute' method.
        """
        if self.dataToDiscretise["nBins"].any() == 0:
            raise ArithmeticError(
                "The total number of bins is zero for one activity, which caused a division by zero."
                "This should not happen because events with length zero should have been dropped."
            )
        self.dataToDiscretise["valPerBin"] = (
            self.dataToDiscretise[self.columnToDiscretise] / self.dataToDiscretise["nBins"]
        )

    def _valueSelect(self):
        """
        Calculate the profile value for each bin for the 'select' method.
        """
        self.dataToDiscretise["valPerBin"] = self.dataToDiscretise[self.columnToDiscretise]

    # FIXME: Implement dynamic battery levels for min and max battery level
    def _valueDynamic(self):
        pass

    def _identifyBins(self):
        """
        Wrapper which identifies the first and the last bin.
        """
        self._identifyFirstBin()
        self._identifyLastBin()

    def _identifyFirstBin(self):
        """
        Identifies every first bin for each activity (trip or parking).
        """
        self.dataToDiscretise["timestampStartCorrected"] = self.dataToDiscretise[
            "timestampStartCorrected"
        ].apply(lambda x: pd.to_datetime(str(x)))
        dayStart = self.dataToDiscretise["timestampStartCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.dataToDiscretise["dailyTimeDeltaStart"] = (
            self.dataToDiscretise["timestampStartCorrected"] - dayStart
        )
        self.dataToDiscretise["startTimeFromMidnightSeconds"] = self.dataToDiscretise[
            "dailyTimeDeltaStart"
        ].apply(lambda x: x.seconds)
        # FIXME: move it out of function globally
        bins = pd.DataFrame({"binTimestamp": self.timeDelta})
        bins.drop(
            bins.tail(1).index, inplace=True
        )  # remove last element, which is zero
        self.binFromMidnightSeconds = bins["binTimestamp"].apply(lambda x: x.seconds)
        self.binFromMidnightSeconds = self.binFromMidnightSeconds + (self.dt * 60)
        self.dataToDiscretise["firstBin"] = (
            self.dataToDiscretise["startTimeFromMidnightSeconds"].apply(
                lambda x: np.argmax(x < self.binFromMidnightSeconds)
            )
        ).astype(int)
        if self.dataToDiscretise["firstBin"].any() > self.nTimeSlots:
            raise ArithmeticError(
                "One of first bin values is bigger than total number of bins."
            )
        if self.dataToDiscretise["firstBin"].unique().any() < 0:
            raise ArithmeticError(
                "One of first bin values is smaller than 0."
            )
        if self.dataToDiscretise["firstBin"].isna().any():
            raise ArithmeticError(
                "One of first bin values is NaN."
            )

    def _identifyLastBin(self):
        """
        Identifies every last bin for each activity (trip or parking).
        """
        dayEnd = self.dataToDiscretise["timestampEndCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.dataToDiscretise["dailyTimeDeltaEnd"] = (
            self.dataToDiscretise["timestampEndCorrected"] - dayEnd
        )
        self.dataToDiscretise["lastBin"] = (
            self.dataToDiscretise["firstBin"] + self.dataToDiscretise["nBins"] - 1
        ).astype(int)
        if self.dataToDiscretise["lastBin"].any() > self.nTimeSlots:
            raise ArithmeticError(
                "One of first bin values is bigger than total number of bins."
            )
        if self.dataToDiscretise["lastBin"].unique().any() < 0:
            raise ArithmeticError(
                "One of first bin values is smaller than 0."
            )
        if self.dataToDiscretise["lastBin"].isna().any():
            raise ArithmeticError(
                "One of first bin values is NaN."
            )

    def _allocateBinShares(self):  # sourcery skip: assign-if-exp
        """
        Wrapper which identifies shared bins and allocates them to a discrestised structure.
        """
        self._overlappingActivities()
        if self.isWeek:
            self.discreteData = self._allocateWeek()
        else:
            self.discreteData = self._allocate()
        self._checkBinValues()

    def _checkBinValues(self):
        """
        Verifies that all bins get a value assigned, otherwise raise an error.
        """
        if self.discreteData.isna().any().any():
            raise Exception("There are NaN in the dataset but shouldn't.")

    # FIXME: Refactor variable names?
    def _dropNoLengthEvents(self):
        """
        Implements a strategy for overlapping bins if time resolution high enough so that the event becomes negligible,
        i.e. drops events with no length (timestampStartCorrected = timestampEndCorrected or activityDuration = 0),
        which cause division by zero in nBins calculation.
        """
        startLength = len(self.dataToDiscretise)
        noLengthActivitiesIDs = self.dataToDiscretise[
            self.dataToDiscretise.activityDuration == pd.Timedelta(
                0)].index.to_list()
        self.IDsWithNoLengthActivities = self.dataToDiscretise.loc[
            noLengthActivitiesIDs]['uniqueID'].unique()
        self.dataToDiscretise = self.dataToDiscretise.drop(
            noLengthActivitiesIDs)
        endLength = len(self.dataToDiscretise)
        droppedActivities = startLength - endLength
        print(f"{droppedActivities} zero-length activities dropped from {len(self.IDsWithNoLengthActivities)} IDs.")
        self._removeActivitiesIfColumnToDiscretiseNoValues()

    # FIXME: Refactor variable names?
    def _removeActivitiesIfColumnToDiscretiseNoValues(self):
        startLength = len(self.dataToDiscretise)
        subsetNoLengthActivitiesIDsOnly = self.dataToDiscretise.loc[
            self.dataToDiscretise.uniqueID.isin(self.IDsWithNoLengthActivities)]
        subsetNoLengthActivitiesIDsOnly = subsetNoLengthActivitiesIDsOnly.set_index("uniqueID", drop=False)
        subsetNoLengthActivitiesIDsOnly.index.names = ['uniqueIDindex']
        IDsWithSumZero = subsetNoLengthActivitiesIDsOnly.groupby(
            ["uniqueID"])[self.columnToDiscretise].sum()
        IDsToDrop = IDsWithSumZero[IDsWithSumZero == 0].index
        self.dataToDiscretise = self.dataToDiscretise.loc[
            ~self.dataToDiscretise.uniqueID.isin(IDsToDrop)]
        endLength = len(self.dataToDiscretise)
        droppedActivities = startLength - endLength
        print(f"Additional {droppedActivities} activities dropped as the sum of all {self.columnToDiscretise}"
              " activities for the specific ID was zero.")

    def _overlappingActivities(self):
        """
        Implements a strategy to treat overlapping bin, especially important for lower time resolution (e.g. 1h).
        """
        # define other strategies to treat overlapping events here
        pass

    def _allocateWeek(self):
        """
        Wrapper method for allocating respective values per bin to days within a week. Expects that the activities
        are formatted in a way that uniqueID represents a unique week ID. The function then loops over the 7 weekdays
        and calls _allocate for each day a total of 7 times.
        """
        raise NotImplementedError()
        # weekSubset = self.dataToDiscretise.groupby(by=["weekdayStr", "actID"])

    def _allocate(self):
        """
        Loops over every activity (row) and allocates the respective value per bin (valPerBin) to each column
        specified in the columns firstBin and lastBin.
        Args:
            weekday (str, optional): _description_. Defaults to None.
        Returns:
            pd.DataFrame: Discretized data set with temporal discretizations in the columns.
        """
        trips = self.dataToDiscretise.copy()
        trips = trips[["uniqueID", "firstBin", "lastBin", "valPerBin"]]
        trips["uniqueID"] = trips["uniqueID"].astype(int)
        return trips.groupby(by="uniqueID").apply(self.assignBins)

    def assignBins(self, vehicleTrips):
        """
        Assigns values for every uniqueID based on first and last bin.
        """
        s = pd.Series(index=range(self.nTimeSlots), dtype=float)
        for _, itrip in vehicleTrips.iterrows():
            start = itrip["firstBin"]
            end = itrip["lastBin"]
            value = itrip["valPerBin"]
            s.loc[start:end] = value
        return s

    # DEPRECATED
    # def assignBinsNp(self, vehicleTrips):
    #     # misses edge case of firstBin=0
    #     s = np.arange(self.nTimeSlots)
    #     for _ , itrip in vehicleTrips.iterrows():
    #         start = itrip['firstBin'] - 1
    #         end = itrip['lastBin']
    #         value = itrip['valPerBin']
    #         s[start: end] = value
    #     return s

    def _writeOutput(self):
        root = Path(self.localPathConfig["pathAbsolute"]["vencoPyRoot"])
        folder = self.globalConfig["pathRelative"]["diaryOutput"]
        fileName = createFileName(
            globalConfig=self.globalConfig,
            manualLabel=self.columnToDiscretise,
            fileNameID="outputDiaryBuilder",
            datasetID=self.datasetID,
        )
        writeOut(data=self.activities, path=root / folder / fileName)

    def discretise(self, column: str):
        self.columnToDiscretise: Optional[str] = column
        print(f"Starting to discretise {self.columnToDiscretise}.")
        self._datasetCleanup()
        self._identifyBinShares()
        self._allocateBinShares()
        # self._writeOutput()
        print(f"Discretisation finished for {self.columnToDiscretise}.")
        self.columnToDiscretise = None
        return self.discreteData
