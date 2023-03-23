__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.03.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import os
import pandas as pd

# print(os.getcwd())

data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')
socStart = 50
upper = 200
lower = 150


def algo(data: pd.DataFrame, socStart: int, lower: int, upper: int):
    delta = -data.drain.fillna(0) + data.maxChargeVolume.fillna(0)
    cDelta = delta.cumsum() + socStart

    while True:
        viol = cDelta[cDelta > upper]
        if len(viol) == 0:
            break
        idx = viol.index[0]
        viol = viol.iloc[0]
        viol = viol - upper
        delta.loc[idx] -= viol
        cDelta = delta.cumsum() + socStart

    print(cDelta)

    while True:
        viol = cDelta[cDelta < lower]
        if len(viol) == 0:
            break
        idx = viol.index[0]
        viol = viol.iloc[0]
        viol = lower - viol
        delta.loc[idx] += viol
        cDelta = delta.cumsum() + socStart

    print(cDelta)


algo(data, socStart=socStart, lower=lower, upper=upper)
# print(data)
