__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scripts.libPlotting import *




def evaluateTripPurposes():
    hourVec = [str(i) for i in range(0, 24)]

    purposes_mid2017_raw = pd.read_csv('./inputData/inputProfiles_Purpose_MiD17.csv')
    purposes_mid2008_raw = pd.read_excel('./inputData/MiD_procCS_caseID-weekday-weight-activity.xlsx', sheet_name='Places')
    purposes_mid2017_raw.set_index('hhPersonID', inplace=True)
    purposes_mid2008_raw.set_index('VEHICLE', inplace=True)
    purpose08_raw = purposes_mid2008_raw.drop(columns=['Weight'])
    purpose17_raw = purposes_mid2017_raw.drop(columns=['tripWeight', 'tripScaleFactor'])
    purpose08_idx = purpose08_raw.set_index('Day', append=True).stack()
    purpose17_idx = purpose17_raw.set_index('ST_WOTAG_str', append=True).stack()
    purpose08 = purpose08_idx.reset_index([1,2])
    purpose17 = purpose17_idx.reset_index([1,2])


    print('end')

if __name__ == '__main__':
    evaluateTripPurposes()