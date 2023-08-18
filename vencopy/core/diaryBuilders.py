__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "01.07.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from vencopy.utils.globalFunctions import createFileName, write_out


class DiaryBuilder:
    def __init__(self, configDict: dict, activities: pd.DataFrame, isWeekDiary: bool = False):
        self.dev_config = configDict["dev_config"]
        self.user_config = configDict["user_config"]
        self.datasetID = configDict["user_config"]["global"]["dataset"]
        self.activities = activities
        self.deltaTime = configDict["user_config"]["diaryBuilders"]["TimeDelta"]
        self.isWeekDiary = isWeekDiary
        self.__updateActivities()
        self.drain = None
        self.chargingPower = None
        self.uncontrolledCharge = None
        self.maxBatteryLevel = None
        self.minBatteryLevel = None
        self.distributor = TimeDiscretiser(
            datasetID=self.datasetID,
            dev_config=self.dev_config,
            user_config=self.user_config,
            activities=self.activities,
            dt=self.deltaTime,
            isWeek=isWeekDiary,
        )

    def __updateActivities(self):
        """
        Updates timestamps and removes activities whose length equals zero to avoid inconsistencies in profiles
        which are separatly discretised (interdependence at single vehicle level of drain, charging power etc i.e.
        no charging available when driving).
        """
        self.__correctTimestamp()
        self.__removesZeroLengthActivities()

    def __correctTimestamp(self) -> pd.DataFrame:
        """
        Rounds timestamps to predifined resolution.
        """
        self.activities["timestampStartCorrected"] = self.activities["timestampStart"].dt.round(f"{self.deltaTime}min")
        self.activities["timestampEndCorrected"] = self.activities["timestampEnd"].dt.round(f"{self.deltaTime}min")
        self.activities["activityDuration"] = (
            self.activities["timestampEndCorrected"] - self.activities["timestampStartCorrected"]
        )
        return self.activities

    def __removesZeroLengthActivities(self):
        """
        Drops line when activity duration is zero, which causes inconsistencies in diaryBuilder (e.g. division by zero in nBins calculation).
        """
        startLength = len(self.activities)
        self.activities = self.activities.drop(
            self.activities[self.activities.activityDuration == pd.Timedelta(0)].index.to_list()
        )
        endLength = len(self.activities)
        print(
            f"{startLength - endLength} activities dropped from {startLength} total activities because activity length equals zero."
        )

    def createDiaries(self):
        start_time = time.time()
        self.drain = self.distributor.discretise(profile=self.drain, profileName="drain", method="distribute")
        self.chargingPower = self.distributor.discretise(
            profile=self.chargingPower, profileName="availablePower", method="select"
        )
        self.uncontrolledCharge = self.distributor.discretise(
            profile=self.uncontrolledCharge, profileName="uncontrolledCharge", method="dynamic"
        )
        self.maxBatteryLevel = self.distributor.discretise(
            profile=self.maxBatteryLevel, profileName="maxBatteryLevelStart", method="dynamic"
        )
        self.minBatteryLevel = self.distributor.discretise(
            profile=self.minBatteryLevel, profileName="minBatteryLevelEnd", method="dynamic"
        )
        needed_time = time.time() - start_time
        print(f"Needed time to discretise all columns: {needed_time}.")

    def uncontrolledCharging(self, maxBatLev: pd.DataFrame) -> pd.DataFrame:
        uncCharge = maxBatLev.copy()
        for cName, c in uncCharge.items():
            if cName > 0:
                tempCol = maxBatLev[cName] - maxBatLev[cName - 1]
                uncCharge[cName] = tempCol.where(tempCol >= 0, other=0)
            else:
                uncCharge[cName] = 0
        return uncCharge


class TimeDiscretiser:
    def __init__(
        self,
        activities: pd.DataFrame,
        dt: int,
        datasetID: str,
        user_config: dict,
        dev_config: dict,
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
        """
        self.activities = activities
        self.datasetID = datasetID
        self.dataToDiscretise = None
        self.user_config = user_config
        self.dev_config = dev_config
        self.quantum = pd.Timedelta(value=1, unit="min")
        self.dt = dt  # e.g. 15 min
        self.isWeek = isWeek
        self.nTimeSlots = int(self.__nSlotsPerInterval(interval=pd.Timedelta(value=self.dt, unit="min")))
        if isWeek:
            self.timeDelta = pd.timedelta_range(start="00:00:00", end="168:00:00", freq=f"{self.dt}T")
            self.weekdays = self.activities["weekdayStr"].unique()
        else:  # is Day
            self.timeDelta = pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.dt}T")
        self.timeIndex = list(self.timeDelta)
        self.discreteData = None

    def __nSlotsPerInterval(self, interval: pd.Timedelta) -> int:
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

    def __datasetCleanup(self):
        self.__removeColumns()
        self.__correctValues()
        self.__correctTimestamp()

    def __removeColumns(self) -> pd.DataFrame:
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
        if self.columnToDiscretise == "uncontrolledCharge":
            necessaryColumns = necessaryColumns + ["availablePower", "timestampEndUC"]
        self.dataToDiscretise = self.activities[necessaryColumns].copy()
        return self.dataToDiscretise

    def __correctValues(self) -> pd.DataFrame:
        """
        Depending on the columns to discretise correct some values.
        - drain profile: pads NaN with 0s
        - uncontrolledCharge profile: instead of removing rows with tripID, assign 0 to rows with tripID
        - residualNeed profile: pads NaN with 0s
        """
        if self.columnToDiscretise == "drain":
            self.dataToDiscretise["drain"] = self.dataToDiscretise["drain"].fillna(0)
        elif self.columnToDiscretise == "uncontrolledCharge":
            self.dataToDiscretise["uncontrolledCharge"] = self.dataToDiscretise["uncontrolledCharge"].fillna(0)
        elif self.columnToDiscretise == "residualNeed":
            self.dataToDiscretise["residualNeed"] = self.dataToDiscretise["residualNeed"].fillna(0)
        return self.dataToDiscretise

    def __correctTimestamp(self) -> pd.DataFrame:
        """
        Rounds timestamps to predifined resolution.
        """
        self.dataToDiscretise["timestampStartCorrected"] = self.dataToDiscretise["timestampStart"].dt.round(
            f"{self.dt}min"
        )
        self.dataToDiscretise["timestampEndCorrected"] = self.dataToDiscretise["timestampEnd"].dt.round(f"{self.dt}min")
        return self.dataToDiscretise

    def __createDiscretisedStructureWeek(self):
        """
        Method for future release working with sampled weeks.

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

    def __identifyBinShares(self):
        """
        Calculates value share to be assigned to bins and identifies the bins.
        Includes a wrapper for the 'distribute', 'select' and 'dynamic' method.
        """
        self.__calculateNBins()
        self.__identifyBins()
        if self.method == "distribute":
            self.__valueDistribute()
        elif self.method == "select":
            self.__valueSelect()
        elif self.method == "dynamic":
            if self.columnToDiscretise in ("maxBatteryLevelStart", "minBatteryLevelEnd"):
                self.__valueNonlinearLevel()
            elif self.columnToDiscretise == "uncontrolledCharge":
                self.__valueNonlinearCharge()
        else:
            raise (
                ValueError(
                    f'Specified method {self.method} is not implemented please specify "distribute" or "select".'
                )
            )

    def __calculateNBins(self):
        """
        Updates the activity duration based on the rounded timstamps.
        Calculates the multiple of dt of the activity duration and stores it to column nBins. E.g. a 2h-activity
        with a dt of 15 mins would have a 8 in the column.
        """
        self.dataToDiscretise["activityDuration"] = (
            self.dataToDiscretise["timestampEndCorrected"] - self.dataToDiscretise["timestampStartCorrected"]
        )
        self.__removesZeroLengthActivities()
        self.dataToDiscretise["nBins"] = self.dataToDiscretise["activityDuration"] / (
            pd.Timedelta(value=self.dt, unit="min")
        )
        if not self.dataToDiscretise["nBins"].apply(float.is_integer).all():
            raise ValueError("Not all bin counts are integers.")
        self.__dropNBinsLengthZero()
        self.dataToDiscretise["nBins"] = self.dataToDiscretise["nBins"].astype(int)

    def __dropNBinsLengthZero(self):
        """
        Drops line when nBins is zero, which cause division by zero in nBins calculation.
        """
        startLength = len(self.dataToDiscretise)
        self.dataToDiscretise.drop(self.dataToDiscretise[self.dataToDiscretise.nBins == 0].index)
        endLength = len(self.dataToDiscretise)
        droppedProfiles = startLength - endLength
        if droppedProfiles != 0:
            raise ValueError(f"{droppedProfiles} activities dropped because bin lenght equals zero.")

    def __valueDistribute(self):
        """
        Calculates the profile value for each bin for the 'distribute' method.
        """
        if self.dataToDiscretise["nBins"].any() == 0:
            raise ArithmeticError(
                "The total number of bins is zero for one activity, which caused a division by zero."
                "This should not happen because events with length zero should have been dropped."
            )
        self.dataToDiscretise["valPerBin"] = (
            self.dataToDiscretise[self.columnToDiscretise] / self.dataToDiscretise["nBins"]
        )

    def __valueSelect(self):
        """
        Calculates the profile value for each bin for the 'select' method.
        """
        self.dataToDiscretise["valPerBin"] = self.dataToDiscretise[self.columnToDiscretise]

    def __valueNonlinearLevel(self):
        """
        Calculates the bin values dynamically (e.g. for the SoC). It returns a
        non-linearly increasing list of values capped to upper and lower battery
        capacity limitations. The list of values is alloacted to bins in the
        function __allocate() in the same way as for value-per-bins. Operates
        directly on class attributes thus neither input nor return attributes.
        """
        self.__deltaBatteryLevelDriving(d=self.dataToDiscretise, valCol=self.columnToDiscretise)
        self.__deltaBatteryLevelCharging(d=self.dataToDiscretise, valCol=self.columnToDiscretise)

    def __deltaBatteryLevelDriving(self, d: pd.DataFrame, valCol: str):
        """Calculates decreasing battery level values for driving activities for
        both cases, minimum and maximum battery level. The cases have to be
        differentiated because the max case runs chronologically from morning to
        evening while the min case runs anti-chronologically from end-of-day to
        beginning. Thus, in the latter case, drain has to be added to the
        battery level.
        The function __increaseLevelPerBin() is applied to the whole data set with
        the respective start battery levels (socStart), battery level increases
        (socAddPerBin) and nBins for each activity respectively in a vectorized
        manner.
        The function adds a column 'valPerBin' to d directly, thus it doesn't
        return anything.

        Args:
            d (pd.DataFrame): Activity data with activities in rows and at least
            the columns valCol, 'drainPerBin', 'valPerBin', 'parkID' and
            'nBins'.
            valCol (str): The column to descritize. Currently only
            maxBatteryLevelStart and minBatteryLevelStart are implemented.
        """
        if valCol == "maxBatteryLevelStart":
            d["drainPerBin"] = (self.activities.drain / d.nBins) * -1
            d["valPerBin"] = d.loc[d["parkID"].isna(), :].apply(
                lambda x: self.__increaseLevelPerBin(
                    socStart=x[valCol], socAddPerBin=x["drainPerBin"], nBins=x["nBins"]
                ),
                axis=1,
            )
        elif valCol == "minBatteryLevelEnd":
            d["drainPerBin"] = self.activities.drain / d.nBins
            d["valPerBin"] = d.loc[d["parkID"].isna(), :].apply(
                lambda x: self.__increaseLevelPerBin(
                    socStart=x[valCol],
                    socAddPerBin=x["drainPerBin"],
                    nBins=x["nBins"],
                ),
                axis=1,
            )

    def __deltaBatteryLevelCharging(self, d: pd.DataFrame, valCol: str):
        """Calculates increasing battery level values for park / charging
        activities for both cases, minimum and maximum battery level. The cases
        have to be differentiated because the max case runs chronologically from
        morning to evening while the min case runs anti-chronologically from
        evening to morning. Thus, in the latter case, charge has to be
        subtracted from the battery level. Charging volumes per bin are
        calculated from the 'availablePower' column in d.
        The function __increaseLevelPerBin() is applied to the whole data set with
        the respective start battery levels (socStart), battery level increases
        (socAddPerBin) and nBins for each activity respectively in a vectorized
        manner. Then, battery capacity limitations are enforced applying the
        function __enforceBatteryLimit().
        The function adds a column 'valPerBin' to d directly, thus it doesn't
        return anything.

        Args:
            d (pd.DataFrame): DataFrame with activities in rows and at least
            the columns valCol, 'availablePower', 'tripID' and
            'nBins'.
            valCol (str): The column to descritize. Currently only
            maxBatteryLevelStart and minBatteryLevelStart are implemented.
        """
        if valCol == "maxBatteryLevelStart":
            d["chargePerBin"] = self.activities.availablePower * self.dt / 60
            d.loc[d["tripID"].isna(), "valPerBin"] = d.loc[d["tripID"].isna(), :].apply(
                lambda x: self.__increaseLevelPerBin(
                    socStart=x[valCol], socAddPerBin=x["chargePerBin"], nBins=x["nBins"]
                ),
                axis=1,
            )
            d.loc[d["tripID"].isna(), "valPerBin"] = d.loc[d["tripID"].isna(), "valPerBin"].apply(
                self.__enforceBatteryLimit,
                how="upper",
                lim=self.user_config["flexEstimators"]["Battery_capacity"]
                * self.user_config["flexEstimators"]["Maximum_SOC"],
            )
        elif valCol == "minBatteryLevelEnd":
            d["chargePerBin"] = self.activities.availablePower * self.dt / 60 * -1
            d.loc[d["tripID"].isna(), "valPerBin"] = d.loc[d["tripID"].isna(), :].apply(
                lambda x: self.__increaseLevelPerBin(
                    socStart=x[valCol], socAddPerBin=x["chargePerBin"], nBins=x["nBins"]
                ),
                axis=1,
            )
            d.loc[d["tripID"].isna(), "valPerBin"] = d.loc[d["tripID"].isna(), "valPerBin"].apply(
                self.__enforceBatteryLimit,
                how="lower",
                lim=self.user_config["flexEstimators"]["Battery_capacity"]
                * self.user_config["flexEstimators"]["Minimum_SOC"],
            )

    def __increaseLevelPerBin(self, socStart: float, socAddPerBin: float, nBins: int) -> list:
        """Returns a list of battery level values with length nBins starting
        with socStart with added value of socAddPerBin.

        Args:
            socStart (float): Starting SOC
            socAddPerBin (float): Consecutive (constant) additions to the start
            SOC
            nBins (int): Number of discretized bins (one per timeslot)

        Returns:
            list: List of nBins increasing battery level values
        """
        tmp = socStart
        lst = [tmp]
        for _ in range(nBins - 1):
            tmp += socAddPerBin
            lst.append(tmp)
        return lst

    def __enforceBatteryLimit(self, deltaBat: list, how: str, lim: float) -> list:
        """Lower-level function that caps a list of values at lower or upper
        (determined by how) limits given by limit. Thus [0, 40, 60] with
        how=upper and lim=50 would return [0, 40, 50].

        Args:
            deltaBat (list): List of float values of arbitrary length.
            how (str): Must be either 'upper' or 'lower'.
            lim (float): Number of threshold to which to limit the values in the
            list.

        Returns:
            list: Returns a list of same length with values limited to lim.
        """
        if how == "lower":
            return [max(i, lim) for i in deltaBat]
        elif how == "upper":
            return [min(i, lim) for i in deltaBat]

    def __valueNonlinearCharge(self):
        self.__ucParking()
        self.__ucDriving()

    def __ucParking(self):
        self.dataToDiscretise["timestampEndUC"] = pd.to_datetime(self.dataToDiscretise["timestampEndUC"])
        self.dataToDiscretise["timedeltaUC"] = (
            self.dataToDiscretise["timestampEndUC"] - self.dataToDiscretise["timestampStart"]
        )
        self.dataToDiscretise["nFullBinsUC"] = (
            self.dataToDiscretise.loc[self.dataToDiscretise["tripID"].isna(), "timedeltaUC"].dt.total_seconds()
            / 60
            / self.dt
        ).astype(int)
        self.dataToDiscretise["valPerBin"] = self.dataToDiscretise.loc[self.dataToDiscretise["tripID"].isna(), :].apply(
            lambda x: self.__chargeRatePerBin(
                chargeRate=x["availablePower"], chargeVol=x["uncontrolledCharge"], nBins=x["nBins"]
            ),
            axis=1,
        )

    def __ucDriving(self):
        self.dataToDiscretise.loc[self.dataToDiscretise["parkID"].isna(), "valPerBin"] = 0

    def __chargeRatePerBin(self, chargeRate: float, chargeVol: float, nBins: int) -> list:
        if chargeRate == 0:
            return [0] * nBins
        chargeRatesPerBin = [chargeRate] * nBins
        volumesPerBin = [r * self.dt / 60 for r in chargeRatesPerBin]
        cEnergy = np.cumsum(volumesPerBin)
        idxsOvershoot = [idx for idx, en in enumerate(cEnergy) if en > chargeVol]

        # Incomplete bin treatment
        if idxsOvershoot:
            binOvershoot = idxsOvershoot.pop(0)
        # uncontrolled charging never completed during activity. This occurs when discretized activity is shorter than
        # original due to discr. e.g. uniqueID == 10040082, parkID==5 starts at 16:10 and ends at 17:00, with dt=15 min
        # it has 3 bins reducing the discretized duration to 45 minutes instead of 50 minutes.
        elif cEnergy[0] < chargeVol:
            return volumesPerBin
        else:  # uncontrolled charging completed in first bin
            return [round(chargeVol, 3)]

        if binOvershoot == 0:
            valLastCBin = round(chargeVol, 3)
        else:
            valLastCBin = round((chargeVol - cEnergy[binOvershoot - 1]), 3)

        return volumesPerBin[:binOvershoot] + [valLastCBin] + [0] * (len(idxsOvershoot))

    def __identifyBins(self):
        """
        Wrapper which identifies the first and the last bin.
        """
        self.__identifyFirstBin()
        self.__identifyLastBin()

    def __identifyFirstBin(self):
        """
        Identifies every first bin for each activity (trip or parking).
        """
        self.dataToDiscretise["timestampStartCorrected"] = self.dataToDiscretise["timestampStartCorrected"].apply(
            lambda x: pd.to_datetime(str(x))
        )
        dayStart = self.dataToDiscretise["timestampStartCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.dataToDiscretise["dailyTimeDeltaStart"] = self.dataToDiscretise["timestampStartCorrected"] - dayStart
        self.dataToDiscretise["startTimeFromMidnightSeconds"] = self.dataToDiscretise["dailyTimeDeltaStart"].apply(
            lambda x: x.seconds
        )
        bins = pd.DataFrame({"binTimestamp": self.timeDelta})
        bins.drop(bins.tail(1).index, inplace=True)  # remove last element, which is zero
        self.binFromMidnightSeconds = bins["binTimestamp"].apply(lambda x: x.seconds)
        self.binFromMidnightSeconds = self.binFromMidnightSeconds + (self.dt * 60)
        self.dataToDiscretise["firstBin"] = (
            self.dataToDiscretise["startTimeFromMidnightSeconds"].apply(
                lambda x: np.argmax(x < self.binFromMidnightSeconds)
            )
        ).astype(int)
        if self.dataToDiscretise["firstBin"].any() > self.nTimeSlots:
            raise ArithmeticError("One of first bin values is bigger than total number of bins.")
        if self.dataToDiscretise["firstBin"].unique().any() < 0:
            raise ArithmeticError("One of first bin values is smaller than 0.")
        if self.dataToDiscretise["firstBin"].isna().any():
            raise ArithmeticError("One of first bin values is NaN.")

    def __identifyLastBin(self):
        """
        Identifies every last bin for each activity (trip or parking).
        """
        dayEnd = self.dataToDiscretise["timestampEndCorrected"].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day)
        )
        self.dataToDiscretise["dailyTimeDeltaEnd"] = self.dataToDiscretise["timestampEndCorrected"] - dayEnd
        self.dataToDiscretise["lastBin"] = (
            self.dataToDiscretise["firstBin"] + self.dataToDiscretise["nBins"] - 1
        ).astype(int)
        if self.dataToDiscretise["lastBin"].any() > self.nTimeSlots:
            raise ArithmeticError("One of first bin values is bigger than total number of bins.")
        if self.dataToDiscretise["lastBin"].unique().any() < 0:
            raise ArithmeticError("One of first bin values is smaller than 0.")
        if self.dataToDiscretise["lastBin"].isna().any():
            raise ArithmeticError("One of first bin values is NaN.")

    def __allocateBinShares(self):  # sourcery skip: assign-if-exp
        """
        Wrapper which identifies shared bins and allocates them to a discrestised structure.
        """
        # self._overlappingActivities()
        self.discreteData = self.__allocateWeek() if self.isWeek else self.__allocate()
        self.__checkBinValues()

    def __checkBinValues(self):
        """
        Verifies that all bins get a value assigned, otherwise raise an error.
        """
        if self.discreteData.isna().any().any():
            raise ValueError("There are NaN in the dataset.")

    def __removesZeroLengthActivities(self):
        """
        Implements a strategy for overlapping bins if time resolution high enough so that the event becomes negligible,
        i.e. drops events with no length (timestampStartCorrected = timestampEndCorrected or activityDuration = 0),
        which cause division by zero in nBins calculation.
        """
        startLength = len(self.dataToDiscretise)
        noLengthActivitiesIDs = self.dataToDiscretise[
            self.dataToDiscretise.activityDuration == pd.Timedelta(0)
        ].index.to_list()
        self.IDsWithNoLengthActivities = self.dataToDiscretise.loc[noLengthActivitiesIDs]["uniqueID"].unique()
        self.dataToDiscretise = self.dataToDiscretise.drop(noLengthActivitiesIDs)
        endLength = len(self.dataToDiscretise)
        droppedActivities = startLength - endLength
        if droppedActivities != 0:
            raise ValueError(
                f"{droppedActivities} zero-length activities dropped from {len(self.IDsWithNoLengthActivities)} IDs."
            )
        self.__removeActivitiesWithZeroValue()

    def __removeActivitiesWithZeroValue(self):
        startLength = len(self.dataToDiscretise)
        subsetNoLengthActivitiesIDsOnly = self.dataToDiscretise.loc[
            self.dataToDiscretise.uniqueID.isin(self.IDsWithNoLengthActivities)
        ]
        subsetNoLengthActivitiesIDsOnly = subsetNoLengthActivitiesIDsOnly.set_index("uniqueID", drop=False)
        subsetNoLengthActivitiesIDsOnly.index.names = ["uniqueIDindex"]
        IDsWithSumZero = subsetNoLengthActivitiesIDsOnly.groupby(["uniqueID"])[self.columnToDiscretise].sum()
        IDsToDrop = IDsWithSumZero[IDsWithSumZero == 0].index
        self.dataToDiscretise = self.dataToDiscretise.loc[~self.dataToDiscretise.uniqueID.isin(IDsToDrop)]
        endLength = len(self.dataToDiscretise)
        droppedActivities = startLength - endLength
        if droppedActivities != 0:
            raise ValueError(
                f"Additional {droppedActivities} activities dropped as the sum of all {self.columnToDiscretise} activities for the specific ID was zero."
            )

    def __allocateWeek(self):
        """
        Wrapper method for allocating respective values per bin to days within a week. Expects that the activities
        are formatted in a way that uniqueID represents a unique week ID. The function then loops over the 7 weekdays
        and calls __allocate for each day a total of 7 times.
        """
        raise NotImplementedError("The method has not been implemneted yet.")

    def __allocate(self) -> pd.DataFrame:
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

    def assignBins(self, acts: pd.DataFrame) -> pd.Series:
        """
        Assigns values for every uniqueID based on first and last bin.
        """
        s = pd.Series(index=range(self.nTimeSlots), dtype=float)
        for _, itrip in acts.iterrows():
            start = itrip["firstBin"]
            end = itrip["lastBin"]
            value = itrip["valPerBin"]
            if self.columnToDiscretise == "minBatteryLevelEnd":
                s.loc[start:end] = value[::-1]
            else:
                s.loc[start:end] = value
        return s

    def __write_output(self):
        if self.user_config["global"]["writeOutputToDisk"]["diaryOutput"]:
            root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
            folder = self.dev_config["global"]["pathRelative"]["diaryOutput"]
            fileName = createFileName(
                dev_config=self.dev_config,
                user_config=self.user_config,
                manualLabel=self.columnToDiscretise,
                fileNameID="outputDiaryBuilder",
                datasetID=self.datasetID,
            )
            write_out(data=self.activities, path=root / folder / fileName)

    def discretise(self, profile, profileName: str, method: str) -> pd.DataFrame:
        self.columnToDiscretise: Optional[str] = profileName
        self.dataToDiscretise = profile
        self.method = method
        print(f"Starting to discretise {self.columnToDiscretise}.")
        startTimeDiaryBuilder = time.time()
        self.__datasetCleanup()
        self.__identifyBinShares()
        self.__allocateBinShares()
        if self.user_config["global"]["writeOutputToDisk"]["diaryOutput"]:
            self.__write_output()
        print(f"Discretisation finished for {self.columnToDiscretise}.")
        elapsedTimeDiaryBuilder = time.time() - startTimeDiaryBuilder
        print(f"Needed time to discretise {self.columnToDiscretise}: {elapsedTimeDiaryBuilder}.")
        self.columnToDiscretise = None
        return self.discreteData
