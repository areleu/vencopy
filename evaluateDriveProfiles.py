__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Some functions
def wavg(data, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = data[avg_name]
    w = data[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()

if __name__ == '__main__':
    dailyMileageGermany2008 = 3.080e9  # pkm/d
    dailyMileageGermany2017 = 3.214e9  # pkm/d
    hourVec = [str(i) for i in range(0,24)]
    driveData_mid2008_raw = pd.read_excel('./inputData/MiD_procCS_caseID-weekday-weight-activity.xlsx', sheet_name='DistancesInKm')
    places_mid2008 = pd.read_excel('./inputData/MiD_procCS_caseID-weekday-weight-activity.xlsx', sheet_name='Places')
    driveData_mid2008_raw.set_index('VEHICLE')
    places_mid2008.set_index('VEHICLE')
    driveData_mid2008 = pd.concat([places_mid2008.loc[:, ['Day', 'Weight']], driveData_mid2008_raw], axis=1)
    driveData_mid2008.loc[:, 'W_HOCH'] = driveData_mid2008.loc[:, 'Weight'] * dailyMileageGermany2008
    driveData_mid2017 = pd.read_csv('./inputData/inputProfiles_Drive_MiD17.csv')


    # Plotting all together
    driveData = pd.DataFrame({'mid08sum': driveData_mid2008.loc[:, hourVec].sum(axis=0)})
    driveData.loc[:, 'mid08simpleAvrg'] = driveData_mid2008.loc[:, hourVec].mean(axis=0)
    # driveData.loc[:, 'mid08wAvrg'] = driveData_mid2008.loc[:, hourVec].apply(lambda x:
    #             sum(x * driveData_mid2008.loc[int(x.name),'Weight']) / sum(driveData_mid2008.loc[:, 'Weight']), axis=0)
    for iCol in hourVec:
        driveData.loc[iCol, 'mid08wAvrg'] =  sum(driveData_mid2008.loc[:, iCol] * driveData_mid2008.loc[:, 'Weight']) / sum(driveData_mid2008.loc[:, 'Weight'])


    driveData.loc[:, 'mid17sum'] = driveData_mid2017.loc[:, hourVec].sum(axis=0)
    driveData.loc[:, 'mid17simpleAvrg'] = driveData_mid2017.loc[:, hourVec].mean(axis=0)
    # driveData.loc[:, 'mid17wAvrg'] = driveData_mid2017.loc[:, hourVec].apply(lambda x:
    #                                                                          sum(x * driveData_mid2017.loc[
    #                                                                              int(x.name), 'W_GEW']) / sum(
    #                                                                              driveData_mid2017.loc[:, 'W_GEW']),
    #                                                                          axis=0)
    for iCol in hourVec:
        driveData.loc[iCol, 'mid17wAvrg'] =  sum(driveData_mid2017.loc[:, iCol] * driveData_mid2017.loc[:, 'W_GEW']) / sum(driveData_mid2017.loc[:, 'W_GEW'])

    fig, ax = plt.subplots(2, 1)
    driveData.loc[:, ['mid08sum', 'mid17sum']].plot.line(ax=ax[0])
    driveData.loc[:, ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[1])
#    plt.show()
    fig.savefig('./output/plotsMIDAna/allDays8vs17.png')

    # Plotting weekday specific
    driveDataWeekday = pd.DataFrame({'mid08sum': driveData_mid2008.groupby('Day').sum().drop(labels=['Weight', 'W_HOCH'], axis=1).stack()})
    driveDataWeekday.loc[:,'mid08simpleAvrg'] = driveData_mid2008.groupby('Day').mean().drop(labels=['Weight', 'W_HOCH'], axis=1).stack()
    for iCol in hourVec:
        for iDay in driveData_mid2008.Day.unique():
            driveDataWeekday.loc[(iDay, iCol), 'mid08wAvrg'] \
                = sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, iCol]
                      * driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight']) \
                  / sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight'])


    driveDataWeekday.loc[:,'mid17sum'] = driveData_mid2017.groupby('ST_WOTAG_str').sum().drop(labels=['W_GEW', 'W_HOCH'], axis=1).stack()
    driveDataWeekday.loc[:,'mid17simpleAvrg'] = driveData_mid2017.groupby('ST_WOTAG_str').mean().drop(labels=['W_GEW', 'W_HOCH'], axis=1).stack()
    for iCol in hourVec:
        for iDay in driveData_mid2017.ST_WOTAG_str.unique():
            driveDataWeekday.loc[(iDay, iCol), 'mid17wAvrg'] \
                = sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'ST_WOTAG_str'] == iDay, iCol]
                      * driveData_mid2017.loc[driveData_mid2017.loc[:, 'ST_WOTAG_str'] == iDay, 'W_GEW']) \
                  / sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'ST_WOTAG_str'] == iDay, 'W_GEW'])

    driveDataWeekday = driveDataWeekday.reset_index(level=0)
    fig, ax = plt.subplots(2, 2)
    driveDataWeekday.loc[driveDataWeekday.loc[:,'Day'] == 'THU', ['mid08sum', 'mid17sum']].plot.line(ax=ax[0,0])
    driveDataWeekday.loc[driveDataWeekday.loc[:,'Day'] == 'THU', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[0,1])
    driveDataWeekday.loc[driveDataWeekday.loc[:,'Day'] == 'SAT', ['mid08sum', 'mid17sum']].plot.line(ax=ax[1,0])
    driveDataWeekday.loc[driveDataWeekday.loc[:,'Day'] == 'SAT', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[1,1])
    plt.show()
    fig.savefig('./output/plotsMIDAna/weekDays8vs17.png')


    print('end')