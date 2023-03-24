__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.03.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import os
import pandas as pd
from pathlib import Path

from snippets.flexEstimator.socMax import algo

from vencopy.core.dataParsers import parseData
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict

# print(os.getcwd())

data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')
socStart = 50
upper = 50
lower = 0

configDict = loadConfigDict(basePath=Path('./vencopy/'))

vpData = parseData(configDict=configDict)
vpData.process()

vpGrid = GridModeler(
    configDict=configDict,
    activities=vpData.activities,
    gridModel="simple",
    forceLastTripHome=True
)
vpGrid.assignGrid()

vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
vpFlex._drain()
vpFlex._maxChargeVolumePerParkingAct()

vpFlex.activities.groupby(by=['hhPersonID']).apply(lambda x: algo(x, socStart=socStart, lower=lower, upper=upper))
