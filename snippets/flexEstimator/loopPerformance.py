__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.03.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import os
import pandas as pd
import numpy as np
from pathlib import Path
from functools import partial
from profilehooks import profile

from vencopy.core.dataParsers.dataParsers import parseData
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import load_configs

# print(os.getcwd())

# data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')
socStart = 25
upper = 50
lower = 0

configDict = load_configs(basePath=Path('./vencopy/'))

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


@profile(immediate=True)
def new(vpFlex):
    for row in vpFlex.activities.rolling(2):
        a = 1
#        thisRow = vpFlex.activities.loc[thisIdx, :]
#        nextRow = vpFlex.activities.loc[nextIdx, :]


new(vpFlex)
print('end')
