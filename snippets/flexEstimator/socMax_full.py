__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.03.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import os
import pandas as pd

from snippets.flexEstimator.socMax import algo
# print(os.getcwd())

data = pd.read_csv('./snippets/flexEstimator/testDay.csv')
# data = pd.read_csv('./testDay.csv')
socStart = 50
upper = 200
lower = 150


algo(data, socStart=socStart, lower=lower, upper=upper)
# print(data)
