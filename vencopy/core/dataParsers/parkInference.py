__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod

import pandas as pd

from vencopy.utils.globalFunctions import replace_vec


class ParkInference:
    def __init__(self, configDict) -> None:
        self.user_config = configDict["user_config"]
        self.activities = None
        self.overnightSplitter = OvernightSplitter()

    def add_parking_rows(self, trips: pd.DataFrame) -> pd.DataFrame:
        """
        Wrapper function generating park activity rows between the trip data from the original MID dataset. Some
        utility attributes are being added such as isFirstActivity, isLastActivity or the uniqueID of the next and
        previous activity. Redundant time observations are dropped after timestamp creation for start and end time of
        each activity. Overnight trips (e.g. extending from 23:00 at survey day to 1:30 on the consecutive day) are
        split up into two trips. The first one extends to the end of the day (00:00) and the other one is appended
        to the activity list before the first parking activity (0:00-1:30). The trip distance is split between the two
        based on the time.

        :param split_overnight_trips: Should trips that end on the consecutive day (not the survey day) be split in two
        trips in such a way that the estimated trip distance the next day is appended in the morning hours of the survey
        day?
        """
        self.trips = trips
        split_overnight_trips = self.user_config["dataParsers"]["split_overnight_trips"]
        self.__copyRows()
        self.__addUtilAttributes()
        self.__addParkActAfterLastTrip()
        self.__adjustParkAttrs()
        self._drop_redundant_cols()
        self.__removeParkActsAfterOvernightTrips()
        self.__adjustParkTimestamps()
        self.__setTripAttrsNAForParkActs()
        self.__addNextAndPrevIDs()
        self.__ONSplitDecider(split=split_overnight_trips)  # ON = overnight
        self.__addTimeDeltaCol()
        self.__uniqueIndex()
        print(
            f'Finished activity composition with {self.trips["tripID"].fillna(0).astype(bool).sum()} trips '
            f'and {self.trips["parkID"].fillna(0).astype(bool).sum()} parking activites.'
        )
        return self.trips

    def __copyRows(self):
        # Adding skeleton duplicate rows for parking activities
        self.trips = pd.concat([self.trips] * 2).sort_index(ignore_index=True)
        self.trips["parkID"] = self.trips["tripID"]
        self.trips.loc[range(0, len(self.trips), 2), "tripID"] = pd.NA
        self.trips.loc[range(1, len(self.trips), 2), "parkID"] = pd.NA

    def __addUtilAttributes(self):
        # Adding additional attribute columns for convenience
        self.trips["uniqueID_prev"] = self.trips["uniqueID"].shift(fill_value=0)
        self.trips["isFirstActivity"] = self.trips["uniqueID_prev"] != self.trips["uniqueID"]
        self.trips["uniqueID_next"] = self.trips["uniqueID"].shift(-1, fill_value=0)
        self.trips["isLastActivity"] = self.trips["uniqueID_next"] != self.trips["uniqueID"]

    def __addParkActAfterLastTrip(self):
        # Adding park activities after last trips
        newIndex = self.trips.index[self.trips.isLastActivity]
        dfAdd = self.trips.loc[newIndex, :]
        dfAdd["tripID"] = pd.NA
        self.trips.loc[newIndex, "isLastActivity"] = False
        dfAdd["parkID"] = self.trips.loc[newIndex, "tripID"] + 1
        self.trips = pd.concat([self.trips, dfAdd]).sort_index()

    def __adjustParkAttrs(self):
        # Setting trip attribute values to zero where tripID == NaN (i.e. for all parking activities)
        self.trips.loc[
            self.trips["tripID"].isna(),
            ["tripDistance", "travelTime", "tripIsIntermodal"],
        ] = pd.NA
        self.trips["colFromIndex"] = self.trips.index
        self.trips = self.trips.sort_values(by=["colFromIndex", "tripID"])

    def _drop_redundant_cols(self):
        # Clean-up of temporary redundant columns
        self.trips.drop(
            columns=[
                "tripStartClock",
                "tripEndClock",
                "tripStartYear",
                "tripStartMonth",
                "tripStartWeek",
                "tripStartHour",
                "tripStartMinute",
                "tripEndHour",
                "tripEndMinute",
                "uniqueID_prev",
                "uniqueID_next",
                "colFromIndex",
            ],
            inplace=True,
        )

    def __removeParkActsAfterOvernightTrips(self):
        # Checking for trips across day-limit and removing respective parking activities
        indexOvernight = self.trips["isLastActivity"] & self.trips["tripEndNextDay"]
        indexOvernight = indexOvernight.loc[indexOvernight]
        self.trips.loc[indexOvernight.index, "isLastActivity"] = True
        self.trips = self.trips.reset_index()

        # Get rid of park activities after overnight trips
        indexMultiDayActivity = self.trips["isLastActivity"] & self.trips["tripEndNextDay"] & self.trips["parkID"]
        self.trips = self.trips.loc[~indexMultiDayActivity, :]

    def __adjustParkTimestamps(self):
        """Adjust the start and end timestamps of the newly added rows. This is done via range index, that is reset at
        the beginning. First and last activities have to be treated separately since their dates have to match with
        their daily activity chain.
        """

        self.trips = self.trips.reset_index()
        parkingActwoFirst, parkingActwoLast = self.__getParkingActsWOFirstAndLast()

        self.__updateParkActStart(parkingActwoFirst=parkingActwoFirst)
        self.__updateParkActEnd(parkingActwoLast=parkingActwoLast)

        self.__updateTimestampFirstParkAct()
        self.__updateTimestampLastParkAct()

        print("Completed park timestamp adjustments.")

    def __getParkingActsWOFirstAndLast(self) -> pd.DataFrame:
        """
        Returns all parking activities except for the last one (return argument 1) and the first one (return argument
        2)

        Return:
            pd.Series: Parking activity indices without the last one
            pd.Series: Parking activity indices without the first one
        """
        parkingAct = ~self.trips["parkID"].isna()
        parkingAct = parkingAct.loc[parkingAct]
        return parkingAct.iloc[1:], parkingAct.iloc[:-1]

    def __updateParkActStart(self, parkingActwoFirst: pd.Series):
        """Updating park start timestamps for newly added rows"""
        set_ts = self.trips.loc[parkingActwoFirst.index - 1, "timestampEnd"]
        set_ts.index = self.trips.loc[parkingActwoFirst.index, "timestampStart"].index
        self.trips.loc[parkingActwoFirst.index, "timestampStart"] = set_ts

    def __updateParkActEnd(self, parkingActwoLast: pd.Series):
        """Updating park end timestamps for newly added rows"""
        set_ts = self.trips.loc[parkingActwoLast.index + 1, "timestampStart"]
        set_ts.index = self.trips.loc[parkingActwoLast.index, "timestampEnd"].index
        self.trips.loc[parkingActwoLast.index, "timestampEnd"] = set_ts

    def __updateTimestampFirstParkAct(self):
        """Updating park end timestamps for last activity in new park rows"""
        idxActs = ~(self.trips["parkID"].isna()) & (self.trips["isFirstActivity"])
        self.trips.loc[idxActs, "timestampStart"] = replace_vec(
            self.trips.loc[idxActs, "timestampEnd"], hour=0, minute=0
        )

    def __updateTimestampLastParkAct(self):
        """Updating park end timestamps for last activity in new park rows"""
        idxActs = ~(self.trips["parkID"].isna()) & (self.trips["isLastActivity"])
        self.trips.loc[idxActs, "timestampEnd"] = replace_vec(
            self.trips.loc[idxActs, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")

    def __setTripAttrsNAForParkActs(self):
        # Set tripEndNextDay to False for all park activities
        self.trips.loc[self.trips["tripID"].isna(), "tripEndNextDay"] = pd.NA

    def __addNextAndPrevIDs(self):
        self.trips.loc[~self.trips["tripID"].isna(), "actID"] = self.trips["tripID"]
        self.trips.loc[~self.trips["parkID"].isna(), "actID"] = self.trips["parkID"]
        self.trips.loc[~self.trips["isLastActivity"], "nextActID"] = self.trips.loc[:, "actID"].shift(-1)
        self.trips.loc[~self.trips["isFirstActivity"], "prevActID"] = self.trips.loc[:, "actID"].shift(1)

    def __ONSplitDecider(self, split: bool):
        """Boolean function that differentiates if overnight trips should be split (split==True) or not (split==False).
        In the latter case, overnight trips identified by the variable 'tripEndNextDay' are excluded from the data set.

        Args:
            split (bool): Should trips that end on the consecutive day (not the survey day) be split in two trips in
            such a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        if split:
            self.trips = self.overnightSplitter.split_overnight_trips(trips=self.trips)
        else:
            self.__setONVarFalseForLastActTrip()
            self.__neglectONTrips()

    def __setONVarFalseForLastActTrip(self):
        """This function treats the edge case of trips being the last activity in the daily activity chain, i.e. trips
        ending exactly at 00:00. They are falsely labelled as overnight trips which is corrected here.

        """
        idxLastActTrips = (self.trips["isLastActivity"]) & ~(self.trips["tripID"].isna())
        idxLastTripEndMidnight = (
            idxLastActTrips
            & (self.trips.loc[idxLastActTrips, "timestampEnd"].dt.hour == 0)
            & (self.trips.loc[idxLastActTrips, "timestampEnd"].dt.minute == 0)
        )
        self.trips_end_next_day_raw = self.trips["tripEndNextDay"]
        self.trips.loc[idxLastTripEndMidnight, "tripEndNextDay"] = False

    def __neglectONTrips(self):
        """
        Removes all overnight trips from the activities data set based on the column 'tripEndNextDay'. Updates
        timestamp end (to 00:00) and isLastActivity for the new last parking activities. Overwrites self.trips.
        """
        # Column for lastActivity setting later
        self.trips["tripEndNextDay_next"] = self.trips["tripEndNextDay"].shift(-1, fill_value=False)

        # Get rid of overnight trips
        idxNoONTrip = ~(self.trips["tripEndNextDay"].fillna(False))
        self.trips = self.trips.loc[idxNoONTrip, :]

        # Update isLastActivity and timestampEnd variables and clean-up column
        idxNewLastAct = self.trips["tripEndNextDay_next"]
        idxNewLastAct = idxNewLastAct.fillna(False).astype(bool)
        self.trips.loc[idxNewLastAct, "isLastActivity"] = True
        self.trips.loc[idxNewLastAct, "timestampEnd"] = replace_vec(
            self.trips.loc[idxNewLastAct, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")
        self.trips = self.trips.drop(columns=["tripEndNextDay_next"])

    def __addTimeDeltaCol(self):
        # Add timedelta column
        self.trips["timedelta"] = self.trips["timestampEnd"] - self.trips["timestampStart"]

    def __uniqueIndex(self):
        self.trips.drop(columns=["level_0"], inplace=True)
        self.trips.reset_index(inplace=True)  # Due to copying and appending rows, the index has to be reset


class OvernightSplitter:
    def __init__(self):
        self.trips = None

    def split_overnight_trips(self, trips: pd.DataFrame) -> pd.DataFrame:
        """Wrapper function for treating edge case trips ending not in the 24 hours of the survey day but stretch
        to the next day. Those overnight (ON) are split up into an evening trip at the regular survey day and a
        morning trip at the next day. Trip distances are split according to the time the person spent on that trip.
        E.g. if a trip lasts from 23:00 to 2:00 the next day and 100 km, the split-up evening trip will last from
        23:00 to 00:00 of the survey day and 33 km and the morning trip from 00:00 to 2:00 and 66 km. In a next step,
        the morning trip is appended to the survey day in the first hours.

        Here, different edge cases occur.
        Edge case 1 (N=5 in MiD17): For trips that overlap with night (early morning) trips at the survey day, e.g. from
        0:30 to 1:00 for the above mentioned example, the morning part of the split overnight trip is completely
        disregarded.
        Edge case 2 (N=3 in MiD17): When overnight mornging split-trips end exactly at the time where the first trip of
        the survey day starts (2:00 in the example), both trips are consolidated to one trip with all attributes of the
        survey trip.
        These edge cases are documented and quantified in issue #358 'Sum of all distances of dataParser at end equals
        sum of all distances after filtering'.
        """
        self.trips = trips

        # Split overnight trips and add next day distance in the morning (tripID=0)
        isONTrip, overnightTripsAdd = self.__getOvernightActs()
        overnightTripsAddTS = self.__adjustONTimestamps(trips=overnightTripsAdd)
        self.__setAllLastActEndTSToZero()
        morningTrips = self.__setONTripIDZero(trips=overnightTripsAddTS)
        morningTrips = self.__adjustMorningTripDistance(overnightTrips=overnightTripsAdd, morningTrips=morningTrips)
        self.__adjustEveningTripDistance(morningTrips=morningTrips, isONTrip=isONTrip)
        self.__setFirstLastActs(morningTrips=morningTrips)
        isPrevFirstActs = self.__getPrevFirstAct(morningTrips=morningTrips)  # Activities that were previously firstActs
        morningTrips_noOverlap, isPrevFirstActs = self.__neglectOverlapMorningTrips(
            morningTrips=morningTrips, isPrevFirstActs=isPrevFirstActs
        )
        morningTrips_add = self.__setNextParkingTSStart(
            morningTrips=morningTrips_noOverlap, isONTrip=isONTrip, isPrevFirstActs=isPrevFirstActs
        )
        self.__addMorningTrips(morningTrips=morningTrips_add)
        self.__removeFirstParkingAct()
        self.__mergeAdjacentTrips()
        # Implement DELTA mileage check of overnight morning split trip distances
        self.__checkAndAssert()
        self.__dropONCol()
        self.__sortActivities()
        return self.trips

    def __getOvernightActs(self) -> tuple[pd.Series, pd.DataFrame]:
        indexOvernightActs = (
            self.trips["isLastActivity"]
            & self.trips["tripEndNextDay"]
            & ~(
                (self.trips["timestampEnd"].dt.hour == 0)
                & (self.trips["timestampEnd"].dt.minute == 0)  # assure that the overnight trip does
            )
        )  # not exactly end at 00:00
        return indexOvernightActs, self.trips.loc[indexOvernightActs, :]

    def __adjustONTimestamps(self, trips: pd.DataFrame) -> pd.DataFrame:
        tripsRes = trips.copy()
        tripsRes["timestampEnd"] = tripsRes.loc[:, "timestampEnd"] - pd.Timedelta(1, "d")
        tripsRes["timestampStart"] = replace_vec(tripsRes.loc[:, "timestampEnd"], hour=0, minute=0)
        return tripsRes

    def __setAllLastActEndTSToZero(self):
        # Set timestamp end of evening part of overnight trip split to 00:00
        self.trips.loc[self.trips["isLastActivity"], "timestampEnd"] = replace_vec(
            self.trips.loc[self.trips["isLastActivity"], "timestampEnd"],
            hour=0,
            minute=0,
        )

    def __setONTripIDZero(self, trips: pd.DataFrame) -> pd.DataFrame:
        trips["tripID"] = 0
        trips["actID"] = 0
        trips["prevActID"] = pd.NA

        # Update next activity ID
        uniqueID = trips["uniqueID"]
        actIdx = self.trips["uniqueID"].isin(uniqueID) & self.trips["isFirstActivity"]
        trips["nextActID"] = self.trips.loc[actIdx, "actID"]

        # Update previous activity ID of previously first activity
        self.trips.loc[actIdx, "prevActID"] = 0
        return trips

    def __adjustMorningTripDistance(self, overnightTrips: pd.DataFrame, morningTrips: pd.DataFrame) -> pd.DataFrame:
        # Splitting the total distance to morning and evening trip time-share dependent
        morningTrips["timedelta_total"] = overnightTrips["timestampEnd"] - overnightTrips["timestampStart"]
        morningTrips["timedelta_morning"] = morningTrips["timestampEnd"] - morningTrips["timestampStart"]
        morningTrips["timeShare_morning"] = morningTrips["timedelta_morning"] / morningTrips["timedelta_total"]
        morningTrips["timeShare_evening"] = (
            morningTrips["timedelta_total"] - morningTrips["timedelta_morning"]
        ) / morningTrips["timedelta_total"]
        morningTrips["totalTripDistance"] = morningTrips["tripDistance"]
        morningTrips["tripDistance"] = morningTrips["timeShare_morning"] * morningTrips["totalTripDistance"]
        return morningTrips

    def __adjustEveningTripDistance(self, morningTrips: pd.DataFrame, isONTrip: pd.Series):
        self.trips.loc[isONTrip, "tripDistance"] = morningTrips["timeShare_evening"] * morningTrips["totalTripDistance"]

    def __setFirstLastActs(self, morningTrips: pd.DataFrame):
        # Setting first and last activities
        morningTrips["isFirstActivity"] = True
        morningTrips["isLastActivity"] = False

    def __getPrevFirstAct(self, morningTrips: pd.DataFrame):
        return self.trips["uniqueID"].isin(morningTrips["uniqueID"]) & self.trips["isFirstActivity"]

    def __neglectOverlapMorningTrips(
        self, morningTrips: pd.DataFrame, isPrevFirstActs: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.Series]:
        # Option 1 of treating overlaps: After concatenation in the end
        firstTripsEnd = self.trips.loc[isPrevFirstActs, "timestampEnd"].copy()
        firstTripsEnd.index = morningTrips.index  # Adjust index for comparison

        # Filter out morning parts of overnight trip split for persons that already have morning trips in that period
        neglectOvernight = firstTripsEnd < morningTrips["timestampEnd"]
        morningTrips_noOverlap = morningTrips.loc[~neglectOvernight, :]

        # Filter out neglected activities from prevFirstActs accordingly
        neglectOvernightIdx = neglectOvernight
        neglectOvernightIdx.index = isPrevFirstActs[isPrevFirstActs].index  # Align index for filtering
        neglectOvernightIdx = neglectOvernightIdx[neglectOvernightIdx]
        isPrevFirstActs[neglectOvernightIdx.index] = False

        return morningTrips_noOverlap, isPrevFirstActs

    def __setNextParkingTSStart(
        self,
        morningTrips: pd.DataFrame,
        isONTrip: pd.Series,
        isPrevFirstActs: pd.DataFrame,
    ) -> pd.DataFrame:
        # Setting start timestamp of previously first activity (parking) to end timestamp of morning split of ON trip
        ts_new = morningTrips.loc[isONTrip, "timestampEnd"]
        ts_new.index = self.trips.loc[isPrevFirstActs, "timestampStart"].index
        self.trips.loc[isPrevFirstActs, "timestampStart"] = ts_new
        self.trips.loc[isPrevFirstActs, "isFirstActivity"] = False

        # Set nextActID column of ON trips to consecutive activity
        return self.__updateNextActID(
            prevFirstActs=self.trips.loc[isPrevFirstActs, :],
            morningTrips=morningTrips,
        )

    def __updateNextActID(self, prevFirstActs: pd.DataFrame, morningTrips: pd.DataFrame) -> pd.DataFrame:
        nextActs = prevFirstActs.loc[prevFirstActs["prevActID"] == 0, "actID"]
        nextActs.index = morningTrips.index
        ret = morningTrips.copy()
        ret.loc[:, "nextActID"] = nextActs
        return ret

    def __addMorningTrips(self, morningTrips: pd.DataFrame):
        # Appending overnight morning trips
        self.trips = pd.concat([self.trips, morningTrips])

    def __removeFirstParkingAct(self):
        # Removing first parking activities for persons where first activity is a trip (starting at 00:00)
        firstParkActs = self.trips.loc[self.trips["parkID"] == 1, :]
        firstTripActs = self.trips.loc[self.trips["tripID"] == 1, :]
        firstTripActs.index = firstParkActs.index  # Aligning trip indices
        idxParkTS = firstParkActs["timestampStart"] == firstTripActs["timestampStart"]
        self.trips = self.trips.drop(idxParkTS[idxParkTS].index)

        # After removing first parking, set first trip to first activity
        self.trips.loc[
            (self.trips["uniqueID"].isin(firstParkActs.loc[idxParkTS, "uniqueID"])) & (self.trips["tripID"] == 1),
            "isFirstActivity",
        ] = True

    def __mergeAdjacentTrips(self):
        """Consolidate overnight morning trips and first trips for the edge case where morning trips of next day
        end exactly at the beginning of the first trip of the survey day. In this case, the morning split of the
        overnight trip is neglected and the beginning of the first trip is set to 00:00. In the MiD17 data set, there
        were 3 occurences of this case all with end times of the overnight trip between 00:00 and 01:00.

        """
        uniqueID = self.__getUniqueIDsToNeglect()
        self.__neglectZeroTripIDFromActivities(id_neglect=uniqueID)
        self.__updateConsolidatedAct(id_neglect=uniqueID)

    def __checkAndAssert(self):
        # Calculates the neglected trip distances from overnight split trips with regular morning trips
        distance = self.trips["tripDistance"].sum() - self.trips.loc[~self.trips["tripID"].isna(), "tripDistance"].sum()
        allTripDistance = self.trips.loc[~self.trips["tripID"].isna(), "tripDistance"].sum()
        ratio = distance / allTripDistance
        print(
            f"From {allTripDistance} km total mileage in the dataset after filtering, {ratio * 100}% were cropped "
            f"because they corresponded to split-trips from overnight trips."
        )
        assert ratio < 0.01

    def __getUniqueIDsToNeglect(self) -> pd.DataFrame:
        """
        Identifies the household person IDs that should be neglected.
        """
        uniqueIDsOvernight = self.trips.loc[self.trips["tripID"] == 0, "uniqueID"]
        acts = self.trips.loc[self.trips["uniqueID"].isin(uniqueIDsOvernight), :]
        actsOvernight = acts.loc[acts["tripID"] == 0, :]
        # Next trip after morning part of overnight split
        actsNextTrip = acts.loc[acts["prevActID"] == 0, :]
        return actsOvernight.loc[~actsOvernight["uniqueID"].isin(actsNextTrip["uniqueID"]), "uniqueID"]

    def __neglectZeroTripIDFromActivities(self, id_neglect: pd.Series):
        """
        This method filters out the activities with the given hhpid and tripID 0.
        """
        boolNeglect = (self.trips["uniqueID"].isin(id_neglect)) & (self.trips["tripID"] == 0)
        self.trips = self.trips.loc[~boolNeglect, :]

    def __updateConsolidatedAct(self, id_neglect: pd.Series):
        """
        This method sets the start timestamp of the firstActivity of all hhpids given as argument to 00:00. Additionally
        the prevActID is set to pd.NA
        """
        idxConsolidatedTrips = (self.trips["uniqueID"].isin(id_neglect)) & (self.trips["isFirstActivity"])
        self.trips.loc[idxConsolidatedTrips, "timestampStart"] = replace_vec(
            self.trips.loc[idxConsolidatedTrips, "timestampStart"],
            hour=0,
            minute=0,
        )
        self.trips.loc[idxConsolidatedTrips, "prevActID"] = pd.NA

    def __dropONCol(self):
        self.trips = self.trips.drop(columns=["tripEndNextDay"])

    def __sortActivities(self):
        self.trips = self.trips.sort_values(by=["uniqueID", "timestampStart"])

