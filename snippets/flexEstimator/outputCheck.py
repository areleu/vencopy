__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.01.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(__file__)))

from pathlib import Path
import matplotlib.pyplot as plt
from vencopy.core.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator

from vencopy.utils.globalFunctions import loadConfigDict


basePath = Path(__file__).parent.parent
configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
               "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
configDict = loadConfigDict(configNames, basePath)
vpData = ParseMiD(configDict=configDict, datasetID='MiD17', debug=False)
vpData.process()

vpGrid = GridModeler(configDict=configDict, datasetID='MiD17', activities=vpData.activities,
                     gridModel='simple')
vpGrid.assignGrid()

vpFlex = FlexEstimator(configDict=configDict, datasetID='MiD17', activities=vpGrid.activities)
vpFlex.estimateTechnicalFlexibility()


# Relevant columns
cols = ['hhPersonID', 'tripID', 'parkID', 'tripDistance', 'timestampStart',
        'timestampEnd', 'availablePower', 'drain', 'maxBatteryLevelStart',
        'maxBatteryLevelEnd', 'minBatteryLevelStart', 'minBatteryLevelEnd']

# General check of variable histograms
vpFlex['drain'].plot.hist()
plt.show()

# Check of mean value of start-hour explicit value
acts = vpFlex.activities.copy()
acts[['maxBatteryLevelStart', 'maxBatteryLevelEnd', 'minBatteryLevelStart', 'minBatteryLevelEnd',
     'uncontrolledCharge']] = acts[['maxBatteryLevelStart', 'maxBatteryLevelEnd', 'minBatteryLevelStart',
                                   'minBatteryLevelEnd', 'uncontrolledCharge']].astype(float)
acts['startHour'] = acts['timestampStart'].dt.hour
acts = acts[['tripID', 'parkID', 'startHour', 'drain', 'travelTime', 'availablePower', 'maxBatteryLevelStart',
             'maxBatteryLevelEnd', 'minBatteryLevelStart', 'minBatteryLevelEnd', 'uncontrolledCharge']]
avg = acts.groupby(by='startHour').mean()

# Normalization basis for the different variables
ref = acts[['drain', 'travelTime', 'availablePower', 'maxBatteryLevelStart', 'maxBatteryLevelEnd',
            'minBatteryLevelStart', 'minBatteryLevelEnd', 'uncontrolledCharge']].sum()

trips = acts.loc[~acts['tripID'].isna(), :]
parkActs = acts.loc[acts['tripID'].isna(), :]

norm = avg.copy()
norm['drain'] = trips[['startHour', 'drain']].groupby(by='startHour').apply(lambda x: x.sum() / ref['drain'])['drain']
norm['travelTime'] = trips[['startHour', 'travelTime']].groupby(by='startHour').apply(
    lambda x: x.sum() / ref['travelTime'])['travelTime']

norm['availablePower'] = parkActs[['startHour', 'availablePower']].groupby(by='startHour').apply(
    lambda x: x.sum() / len(x))['availablePower']

n = round(len(parkActs) * 0.05)
norm['maxBatteryLevelStart'] = parkActs[['startHour', 'maxBatteryLevelStart']].groupby(by='startHour').apply(
    lambda x: x.nlargest(n=n, columns='maxBatteryLevelStart').min())['maxBatteryLevelStart']
norm['maxBatteryLevelEnd'] = parkActs[['startHour', 'maxBatteryLevelEnd']].groupby(by='startHour').apply(
    lambda x: x.nlargest(n=n, columns='maxBatteryLevelEnd').min())['maxBatteryLevelEnd']
norm['minBatteryLevelStart'] = parkActs[['startHour', 'minBatteryLevelStart']].groupby(by='startHour').apply(
    lambda x: x.nsmallest(n=n, columns='minBatteryLevelStart').max())['minBatteryLevelStart']
norm['minBatteryLevelEnd'] = parkActs[['startHour', 'minBatteryLevelEnd']].groupby(by='startHour').apply(
    lambda x: x.nsmallest(n=n, columns='minBatteryLevelEnd').max())['minBatteryLevelEnd']


# Checking batteryLevelMaxEnd for a variety of profiles
df = vpFlex.activities.loc[(vpFlex.activities['genericID'].isin(
    [10002161, 10002341, 10003121, 10003122, 10003124, 10006381, 10006651, 10009841,
     10010391, 10010393, 10013201, 10013661])) & (~vpFlex.activities['parkID'].isna()),
    ['genericID', 'parkID', 'maxBatteryLevelEnd']].set_index(['genericID', 'parkID']).unstack(
    'genericID')
