__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.03.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import os
import numpy as np
import pandas as pd

# print(os.getcwd())

data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')


def algo(data: pd.DataFrame, socStart: int, lower: int, upper: int):
    delta = -data.drain.fillna(0) + data.maxChargeVolume.fillna(0)
    cDelta = delta.cumsum() + socStart

    deltaSign = delta.apply(np.sign)

    while True:
        overshoot = cDelta[cDelta > upper]
        undershoot = cDelta[cDelta < lower]
        if len(overshoot) + len(undershoot) == 0:
            break

        if len(overshoot) > 0:
            idx = overshoot.index[0]
            overshoot = overshoot.iloc[0]
            overshoot = overshoot - upper
            sigDelta = np.sign(delta.loc[idx])
            delta.loc[idx] -= overshoot
            assert sigDelta == deltaSign.loc[idx]
            cDelta = delta.cumsum() + socStart

        if len(undershoot) > 0:
            idx = undershoot.index[0]
            undershoot = undershoot.iloc[0]
            undershoot = lower - undershoot
            sigDelta = np.sign(delta.loc[idx])
            delta.loc[idx] += undershoot
            if abs(delta.loc[idx]) < 1e-6:
                delta.loc[idx] = 0
            assert sigDelta == deltaSign.loc[idx]
            cDelta = delta.cumsum() + socStart

            if cDelta[idx] > upper:
                raise ArithmeticError(f'Infeasible upper ({upper}) and lower ({lower}) constraints for maximum battery '
                                      f'level. Last battery level: {cDelta}')

    print(delta)
    print(cDelta)


# algo(data, socStart=socStart, lower=lower, upper=upper)
# print(data)
