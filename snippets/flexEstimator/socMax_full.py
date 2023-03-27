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

from snippets.flexEstimator.socMax import algo

from vencopy.core.dataParsers import parseData
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict

# print(os.getcwd())

data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')
socStart = 25
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

# Old imperformant implementation (11.7 s for 20k rows of data)
# pAlgo = partial(algo, socStart=socStart, lower=lower, upper=upper)
# pAlgo.__name__ = 'pAlgo'
# pAlgo.__code__ = algo.__code__
# pAlgo = profile(pAlgo)
# res = vpFlex.activities.groupby(by=['hhPersonID']).apply(pAlgo)
# print(res.head(30))

# exit()


# New more performant implementation of algo looking at the complete dataset
@profile(immediate=True)
def algo(data: pd.DataFrame, socStart: int, lower: int, upper: int):
    data['delta'] = -data.drain.fillna(0) + data.maxChargeVolume.fillna(0)
    cDelta = data[['hhPersonID', 'delta']].groupby(by=['hhPersonID']).cumsum() + socStart

    # deltaSign = data['delta'].apply(np.sign)

    while True:
        overshoot = cDelta[cDelta > upper].fillna(0)

        # overshoot['hhPersonID'] = data['hhPersonID']
        # undershoot['hhPersonID'] = data['hhPersonID']

        if len(overshoot) > 0:
            # First idea not working
            # fOvershoot = overshoot.groupby(by='hhPersonID', as_index=False, dropna=False).first()
            # fIdx = fOvershoot.index
            # overshoot = overshoot - upper

            # FIXME: Treat falsely decreased SOC due to overshoot of previous activity
            # Second idea
            overshoot = overshoot - upper
            # idx_os = ~overshoot.isna()['delta']
            data['delta'] = data['delta'] - overshoot['delta']
            cDelta = data[['hhPersonID', 'delta']].groupby(by=['hhPersonID']).cumsum() + socStart
            
            undershoot = cDelta[cDelta < lower].fillna(0)

            # sigDelta = np.sign(data['delta'].loc[data['hhPersonID'].isin(idxOver.index)])
            # data.iloc[idxOver.index, 'delta'] -= idxOver
            # assert sigDelta == deltaSign.loc[idx]
            # cDelta = data['delta'].cumsum() + socStart

        if len(undershoot) > 0:
            undershoot = lower - undershoot  # A positive number
            # idx_us = ~undershoot.isna()['delta']
            data['delta'] = data['delta'] + undershoot['delta']
            cDelta = data[['hhPersonID', 'delta']].groupby(by=['hhPersonID']).cumsum() + socStart

            # if cDelta[idx] > upper:
            #     raise ArithmeticError(f'Infeasible upper ({upper}) and lower ({lower}) constraints for maximum battery '
            #                           f'level. Last battery level: {cDelta}')

        print((overshoot + undershoot).abs().sum().sum())
        if (overshoot + undershoot).abs().sum().sum() < 1e-6:
            break

    print(data[['hhPersonID', 'parkID', 'tripID', 'delta']])
    print(cDelta)


algo(vpFlex.activities, socStart=socStart, lower=lower, upper=upper)
vpFlex.writeOutput()
