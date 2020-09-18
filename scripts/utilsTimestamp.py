__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import numpy as np

def tripDuration(timestampStart, timestampEnd):
    return timestampEnd - timestampStart

def calcHourShareStart(timestampStart, timestampEnd, duration):
    isSameHourTrip = timestampStart.dt.hour == timestampEnd.dt.hour
    shareSameHour = (timestampEnd.dt.minute - timestampStart.dt.minute) / (duration.dt.seconds / 60)
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
    numberOfHours = timestampEnd.dt.hour - (timestampStart.dt.hour + 1)
    return numberOfHours.where(numberOfHours >= 0, other=0)

def calcFullHourTripLength(duration, numberOfFullHours, tripLength):
    return (numberOfFullHours / (duration.dt.seconds / 3600)) * tripLength

def initiateHourDataframe(indexCol, nHours):
    """
    Sets up an empty dataframe to be filled with hourly data.

    :param indexCol: List of column names
    :param nHours: integer giving the number of columns that should be added to the dataframe
    :return: dataframe with columns given and nHours additional columns appended with 0s
    """
    emptyDf = pd.DataFrame(index=indexCol, columns=range(nHours))
    return(emptyDf)


