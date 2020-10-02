__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import numpy as np
import warnings

def tripDuration(timestampStart, timestampEnd):
    return timestampEnd - timestampStart

def calcHourShareStart(timestampStart, timestampEnd, duration):
    isSameHourTrip = timestampStart.dt.hour == timestampEnd.dt.hour
    shareSameHour = (timestampEnd.dt.minute - timestampStart.dt.minute) / (duration.dt.seconds / 60)
    shareSameHour[duration == pd.Timedelta(0)] = 1  # Set share of first hour to 1 for trips with reported duration of 0
    share = shareSameHour.where(isSameHourTrip, (60 - timestampStart.dt.minute) / (duration.dt.seconds / 60))
    return share, isSameHourTrip

def calcHourShareEnd(timestampEnd, duration, isSameHourTrip):
    share = timestampEnd.dt.minute / (duration.dt.seconds / 60)
    return share.where(~isSameHourTrip, 0)

def calcDistanceShares(data, duration):
    shareHourStart, isSameHourTrip = calcHourShareStart(data.loc[:, 'timestamp_st'], data.loc[:, 'timestamp_en'],
                                                        duration)
    shareHourEnd = calcHourShareEnd(data.loc[:, 'timestamp_en'], duration, isSameHourTrip=isSameHourTrip)
    return shareHourStart, shareHourEnd

def numberOfFullHours(timestampStart, timestampEnd):
    timedeltaTrip = timestampEnd - timestampStart
    numberOfHours = timedeltaTrip.apply(lambda x: x.components.hours)
    numberOfDays = timedeltaTrip.apply(lambda x: x.components.days)
    minLeftFirstHour = pd.to_timedelta(60 - timestampStart.dt.minute, unit='m')
    hasFullHourAfterFirstHour = (timedeltaTrip - minLeftFirstHour) >= pd.Timedelta(1, unit='h')
    numberOfHours = numberOfHours.where(hasFullHourAfterFirstHour, other=0)
    return numberOfHours.where(numberOfDays != -1, other=0)

def calcFullHourTripLength(duration, numberOfFullHours, tripLength):
    fullHourTripLength = (numberOfFullHours / (duration.dt.seconds / 3600)) * tripLength
    fullHourTripLength.loc[duration == pd.Timedelta(0)] = 0  # set trip length to 0 that would otherwise be NaN
    return fullHourTripLength

def initiateHourDataframe(indexCol, nHours):
    """
    Sets up an empty dataframe to be filled with hourly data.

    :param indexCol: List of column names
    :param nHours: integer giving the number of columns that should be added to the dataframe
    :return: dataframe with columns given and nHours additional columns appended with 0s
    """
    emptyDf = pd.DataFrame(index=indexCol, columns=range(nHours))
    return(emptyDf)


def determinePurposeStartHour(departure, arrival):
    if departure.hour == arrival.hour:
        if arrival.minute >= 30:  # Cases 3, 4, 5
            startHour = departure.hour + 1  # Cases 3,5
        else:  # arrival.minute < 30:
            startHour = departure.hour  # Case 4
    else:  # inter-hour trip
        if arrival.minute <= 30:
            startHour = arrival.hour  # Cases 1a and b
        else:  # arrival.minute > 30:
            startHour = arrival.hour + 1  # Cases 2a and b
    return startHour


def determinePurposeHourRange(departure, arrival):
    tripDuration = arrival-departure
    startHour = determinePurposeStartHour(departure, tripDuration)
    return range(startHour, endHour)