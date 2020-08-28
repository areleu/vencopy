# -*- coding:utf-8 -*-

__version__ = '0.0.7'
__maintainer__ = 'Niklas Wulff 16.04.2020'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '24.02.2020'
__status__ = 'test'  # options are: dev, test, prod

# This script holds the function definitions for calculating the five profiles for describing
# electric vehicle consumptions in VencoPy.

import numpy as np
from random import seed
from random import random
from .libLogging import logit
from .libLogging import logger


@logit
def calcConsumptionProfiles(driveProfiles, scalars):
    """
    Calculates electrical consumption profiles from drive profiles assuming specific consumption (in kWh/100 km)
    given in scalar input data file.

    :param driveProfiles: indexed profile file
    :param Scalars: dataframe holding technical assumptions
    :return: Returns a dataframe with consumption profiles in kWh/h in same format and length as driveProfiles but
    scaled with the specific consumption assumption.
    """

    consumptionProfiles = driveProfiles.copy()
    # review have you considered the pandas .astype() method? It is more performant than a direct float type cast.
    # review the division by int 100 can be changed to float 100. which would force python above 2.7 to use float
    # division and thus a typecast might not even be necessary
    # consumptionProfiles = consumptionProfiles * float(scalars.loc['Electric consumption NEFZ', 'value']) / 100
    consumptionProfiles = consumptionProfiles * scalars.loc['Electric consumption NEFZ', 'value'] / float(100)
    return consumptionProfiles


@logit
def calcChargeProfiles(plugProfiles, scalars):
    '''
    Calculates the maximum possible charge power based on the plug profile assuming the charge column power
    given in the scalar input data file (so far under Panschluss).

    :param plugProfiles: indexed boolean profiles for vehicle connection to grid
    :param scalars: VencoPy scalar dataframe
    :return: Returns scaled plugProfile in the same format as plugProfiles.
    '''

    chargeProfiles = plugProfiles.copy()
    # review have you considered the pandas .astype() method? It is more performant than a direct float type cast.
    chargeProfiles = chargeProfiles * float(scalars.loc['Rated power of charging column', 'value'])
    return chargeProfiles


@logit
def calcChargeMaxProfiles(chargeProfiles, consumptionProfiles, scalars, scalarsProc, nIter):
    """
    Calculates all maximum SoC profiles under the assumption that batteries are always charged as soon as they
    are plugged to the grid. Values are assured to not fall below SoC_min * battery capacity or surpass
    SoC_max * battery capacity. Relevant profiles are chargeProfile and consumptionProfile. An iteration assures
    the boundary condition of chargeMaxProfile(0) = chargeMaxProfile(len(profiles)). The number of iterations
    is given as parameter.

    :param chargeProfiles: Indexed dataframe of charge profiles.
    :param consumptionProfiles: Indexed dataframe of consumptionProfiles.
    :param scalars: DataFrame holding techno-economic assumptions.
    :param scalarsProc: DataFrame holding information about profile length and number of hours.
    :param nIter: Number of iterations to assure that the minimum and maximum value are approximately the same
    :return: Returns an indexed DataFrame with the same length and form as chargProfiles and consumptionProfiles,
    containing single-profile SOC max values for each hour in each profile.
    """

    chargeMaxProfiles = chargeProfiles.copy()
    chargeProfiles = chargeProfiles.copy()
    consumptionProfiles = consumptionProfiles.copy()
    batCapMin = scalars.loc['Battery capacity', 'value'] * scalars.loc['Minimum SOC', 'value']
    batCapMax = scalars.loc['Battery capacity', 'value'] * scalars.loc['Maximum SOC', 'value']
    nHours = scalarsProc['noHours']
    idxIt = 1
    for idxIt in range(nIter):
        # ToDo: np.where() replace by pd.something(),
        # ToDo: prohibit typecasting str(idx) in data preparation step colnames as integers {smell} in function indexProfiles()
        for idx in range(nHours):
            # testing line
            # chargeMaxProfiles.ix[3, '0'] = 15.0
            if idx == 0:
                chargeMaxProfiles[str(idx)] = np.where(chargeMaxProfiles[str(idx)] <= batCapMax,
                                                      chargeMaxProfiles[str(nHours - 1)],
                                                      batCapMax)
            else:
                # Calculate and append column with new SoC Max value for comparison and cleaner code
                chargeMaxProfiles['newCharge'] = chargeMaxProfiles[str(idx - 1)] + \
                                                chargeProfiles[str(idx)] - \
                                                consumptionProfiles[str(idx)]

                # Ensure that chargeMaxProfiles values are between batCapMin and batCapMax
                chargeMaxProfiles[str(idx)] = np.where(chargeMaxProfiles['newCharge'] <= batCapMax,
                                                      chargeMaxProfiles['newCharge'],
                                                      batCapMax)
                chargeMaxProfiles[str(idx)] = np.where(chargeMaxProfiles[str(idx)] >= batCapMin,
                                                      chargeMaxProfiles[str(idx)],
                                                      batCapMin)

        # review: general remark instead of str(0) which is a performance heavy operation, one could write "0" which is equivalent and less performance heavy (performance impact is however negligible I guess)
        devCrit = chargeMaxProfiles[str(nHours - 1)].sum() - chargeMaxProfiles[str(0)].sum()
        print(devCrit)
        idxIt += 1
    chargeMaxProfiles.drop(labels='newCharge', axis='columns', inplace=True)
    return chargeMaxProfiles


@logit
def calcChargeProfilesUncontrolled(chargeMaxProfiles, scalarsProc):
    """
    Calculates uncontrolled electric charging based on SoC Max profiles for each hour for each profile.

    :param chargeMaxProfiles: Dataframe holding timestep dependent SOC max values for each profile.
    :param scalarsProc: VencoPy Dataframe holding meta-information about read-in profiles.
    :return: Returns profiles for uncontrolled charging under the assumption that charging occurs as soon as a
    vehicle is connected to the grid up to the point that the maximum battery SOC is reached or the connection
    is interrupted. DataFrame has the same format as chargeMaxProfiles.
    """

    chargeMaxProfiles = chargeMaxProfiles.copy()
    chargeProfilesUncontrolled = chargeMaxProfiles.copy()
    nHours = scalarsProc['noHours']

    for idx in range(nHours):

        if idx != 0:
            chargeProfilesUncontrolled[str(idx)] = np.where(
                chargeMaxProfiles[str(idx)] >= chargeMaxProfiles[str(idx - 1)],
                chargeMaxProfiles[str(idx)] - chargeMaxProfiles[str(idx - 1)],
                0)

    # set value of uncontrolled charging for first hour to average between hour 1 and hour 23
    # because in calcChargeMax iteration the difference is minimized.
    chargeProfilesUncontrolled[str(0)] = \
        (chargeProfilesUncontrolled[str(1)] + chargeProfilesUncontrolled[str(nHours - 1)]) / 2
    return chargeProfilesUncontrolled


@logit
def calcDriveProfilesFuelAux(chargeMaxProfiles, chargeProfilesUncontrolled, driveProfiles, scalars, scalarsProc):
    #ToDo: alternative vectorized format for looping over columns? numpy, pandas: broadcasting-rules
    """
     Calculates necessary fuel consumption profile of a potential auxilliary unit (e.g. a gasoline motor) based
    on gasoline consumption given in scalar input data (in l/100 km). Auxilliary fuel is needed if an hourly
    mileage is higher than the available SoC Max in that hour.

    :param chargeMaxProfiles: Dataframe holding hourly maximum SOC profiles in kWh for all profiles
    :param chargeProfilesUncontrolled: Dataframe holding hourly uncontrolled charging values in kWh/h for all profiles
    :param driveProfiles: Dataframe holding hourly electric driving demand in kWh/h for all profiles.
    :param scalars: Dataframe holding technical assumptions
    :param scalarsProc: Dataframe holding meta-infos about the input
    :return: Returns a DataFrame with single-profile values for back-up fuel demand in the case a profile cannot
    completely be fulfilled with electric driving under the given consumption and battery size assumptions.
    """

    # review: the hardcoding of the column names can cause a lot of problems for people later on if we do not ship the date with the tool.
    # I would recommend to move these column names to a config file similar to i18n strategies
    consumptionPower = scalars.loc['Electric consumption NEFZ', 'value']
    consumptionFuel = scalars.loc['Fuel consumption NEFZ', 'value']

    # initialize data set for filling up later on
    driveProfilesFuelAux = chargeMaxProfiles.copy()
    nHours = scalarsProc['noHours']

    # review (resolved): have you considered naming idx into ihour as it actually contains the currently processed hour and would make the code more readable
    for iHour in range(nHours):
        # review as far as I can tell, the hour 0 is never filled or added as a column to driveProfilesFuelAux. But this should raise an error in line 336 for idx 1. Why does this work anyhow?
        if iHour != 0:
            driveProfilesFuelAux[str(iHour)] = (consumptionFuel / consumptionPower) * \
                                             (driveProfiles[str(iHour)] * consumptionPower / 100 -
                                              chargeProfilesUncontrolled[str(iHour)] -
                                              (chargeMaxProfiles[str(iHour - 1)] - chargeMaxProfiles[str(iHour)]))

    # Setting value of hour=0 equal to the average of hour=1 and last hour
    driveProfilesFuelAux[str(0)] = (driveProfilesFuelAux[str(nHours - 1)] + driveProfilesFuelAux[str(1)]) / 2
    driveProfilesFuelAux = driveProfilesFuelAux.round(4)
    return driveProfilesFuelAux


@logit
def calcChargeMinProfiles(chargeProfiles, consumptionProfiles, driveProfilesFuelAux, scalars, scalarsProc, nIter):
    #ToDo param minSecurityFactor
    """
    Calculates minimum SoC profiles assuming that the hourly mileage has to exactly be fulfilled but no battery charge
    is kept inspite of fulfilling the mobility demand. It represents the minimum charge that a vehicle battery has to
    contain in order to fulfill all trips.
    An iteration is performed in order to assure equality of the SoCs at beginning and end of the profile.

    :param chargeProfiles: Charging profiles with techno-economic assumptions on connection power.
    :param consumptionProfiles: Profiles giving consumed electricity for each trip in each hour assuming specified
        consumption.
    :param driveProfilesFuelAux: Auxilliary fuel demand for fulfilling trips if purely electric driving doesn't suffice.
    :param scalars: Techno-economic input assumptions such as consumption, battery capacity etc.
    :param scalarsProc: Number of profiles and number of hours of each profile.
    :param nIter: Gives the number of iterations to fulfill the boundary condition of the SoC equalling in the first
        and in the last hour of the profile.
    :return: Returns an indexed DataFrame containing minimum SOC values for each profile in each hour in the same
        format as chargeProfiles, consumptionProfiles and other input parameters.
    """

    # review general remark: white spaces in column names give me the creeps as it is easy to mistype
    # and create all kind of wired errors.
    # Especially if there are columns with similar names only differing in whitespaces.
    # This is clearly not the case here, but did you consider naming columns with underscores for easier reference?
    chargeMinProfiles = chargeProfiles.copy()
    batCapMin = scalars.loc['Battery capacity', 'value'] * scalars.loc['Minimum SOC', 'value']
    batCapMax = scalars.loc['Battery capacity', 'value'] * scalars.loc['Maximum SOC', 'value']
    consElectric = scalars.loc['Electric consumption NEFZ', 'value']
    consGasoline = scalars.loc['Fuel consumption NEFZ', 'value']
    nHours = scalarsProc['noHours']
    idxIt = 1
    while idxIt <= nIter:
        for idx in range(nHours):

            # review (resolved) the above nHours implies, that the number of hours can vary based on user input or the
            # underlying data. It seems to me risky to hardcode 23 here if the last hour is meant.
            # Would it not be more prudent to use a variable lastHour that is nHours-1?
            if idx == nHours-1:
                chargeMinProfiles[str(idx)] = np.where(batCapMin <= chargeMinProfiles[str(idx)],
                                                       chargeMinProfiles[str(0)],
                                                       batCapMin)
            else:
                # Calculate and append column with new SOC Max value for comparison and nicer code
                chargeMinProfiles['newCharge'] = chargeMinProfiles[str(idx + 1)] + \
                                                 consumptionProfiles[str(idx + 1)] - \
                                                 chargeProfiles[str(idx + 1)] - \
                                                 (driveProfilesFuelAux[str(idx + 1)] * consElectric / consGasoline)

                # Ensure that chargeMinProfiles values are between batCapMin and batCapMax
                chargeMinProfiles[str(idx)] = np.where(chargeMinProfiles['newCharge'] >= batCapMin,
                                                       chargeMinProfiles['newCharge'],
                                                       batCapMin)
                chargeMinProfiles[str(idx)] = np.where(chargeMinProfiles[str(idx)] <= batCapMax,
                                                       chargeMinProfiles[str(idx)],
                                                       batCapMax)

        # FixMe Are these 2 lines of further use?
        devCrit = chargeMinProfiles[str(nHours - 1)].sum() - chargeMinProfiles[str(0)].sum()
        print(devCrit)

        idxIt += 1
    chargeMinProfiles.drop('newCharge', axis='columns', inplace=True)
    return chargeMinProfiles


@logit
def createRandNo(driveProfiles, setSeed=1):
    """
    Creates a random number between 0 and 1 for each profile based on driving profiles.

    :param driveProfiles: Dataframe holding hourly electricity consumption values in kWh/h for all profiles
    :param setSeed: Seed for reproducing stochasticity. Scalar number.
    :return: Returns an indexed series with the same indices as dirveProfiles with a random number between 0 and 1 for
    each index.
    """

    idxData = driveProfiles.copy()
    seed(setSeed)  # seed random number generator for reproducibility
    idxData['randNo'] = np.random.random(len(idxData))
    idxData['randNo'] = [random() for _ in range(len(idxData))]  # generate one random number for each profile / index
    randNo = idxData.loc[:, 'randNo']
    return randNo


@logit
def calcProfileSelectors(chargeProfiles,
                         consumptionProfiles,
                         driveProfiles,
                         driveProfilesFuelAux,
                         randNos,
                         scalars,
                         fuelDriveTolerance,
                         isBEV):
    """
    This function calculates two filters. The first filter, filterCons, excludes profiles that depend on auxiliary
    fuel with an option of a tolerance and those that don't reach a minimum daily average for mileage.
    A second filter filterDSM excludes profiles where the battery doesn't suffice the mileage and those where charging
    throughout the day supplies less energy than necessary for the respective trips.

    :param chargeProfiles: Indexed DataFrame giving hourly charging profiles
    :param consumptionProfiles: Indexed DataFrame giving hourly consumption profiles
    :param driveProfiles:  Indexed DataFrame giving hourly electricity demand profiles for driving.
    :param driveProfilesFuelAux: Indexed DataFrame giving auxiliary fuel demand.
    :param randNos: Indexed Series giving a random number between 0 and 1 for each profiles.
    :param scalars: Techno-economic assumptions
    :param fuelDriveTolerance: Give a threshold value how many liters may be needed throughout the course of a day
    in order to still consider the profile.
    :param isBEV: Boolean value. If true, more 2030 profiles are taken into account (in general).
    :return: The bool indices are written to one DataFrame in the DataManager with the columns randNo, indexCons and
    indexDSM and the same indices as the other profiles.
    """

    boolBEV = scalars.loc['Is BEV?', 'value']
    minDailyMileage = scalars.loc['Minimum daily mileage', 'value']
    batSize = scalars.loc['Battery capacity', 'value']
    socMax = scalars.loc['Maximum SOC', 'value']
    socMin = scalars.loc['Minimum SOC', 'value']
    filterCons = driveProfiles.copy()
    filterCons['randNo'] = randNos
    filterCons['bolFuelDriveTolerance'] = driveProfilesFuelAux.sum(axis='columns') * \
                                           boolBEV < fuelDriveTolerance
    filterCons['bolMinDailyMileage'] = driveProfiles.sum(axis='columns') > \
                                        (2 * randNos * minDailyMileage +
                                         (1 - randNos) * minDailyMileage *
                                         isBEV)
    filterCons['indexCons'] = filterCons.loc[:, 'bolFuelDriveTolerance'] & \
                               filterCons.loc[:, 'bolMinDailyMileage']
    filterCons['bolConsumption'] = consumptionProfiles.sum(axis=1) < \
                                    chargeProfiles.sum(axis=1)
    filterCons['bolSuffBat'] = consumptionProfiles.sum(axis=1) < \
                                batSize * (socMax - socMin)
    filterCons['indexDSM'] = filterCons['indexCons'] & filterCons['bolConsumption'] & filterCons['bolSuffBat']

    print('There are ' + str(sum(filterCons['indexCons'])) + ' considered profiles and ' + \
                    str(sum(filterCons['indexDSM'])) + ' DSM eligible profiles.')
    filterCons_out = filterCons.loc[:, ['randNo', 'indexCons', 'indexDSM']]
    return filterCons_out


@logit
def calcElectricPowerProfiles(consumptionProfiles, driveProfilesFuelAux, scalars, filterCons, scalarsProc,
                              filterIndex):
    """
    Calculates electric power profiles that serve as outflow of the fleet batteries.

    :param consumptionProfiles: Indexed DataFrame containing electric vehicle consumption profiles.
    :param driveProfilesFuelAux: Indexed DataFrame containing
    :param scalars: VencoPy Dataframe containing technical assumptions
    :param filterCons: Dataframe containing one boolean filter value for each profile
    :param scalarsProc: Dataframe containing meta information of input profiles
    :param filterIndex: Can be either 'indexCons' or 'indexDSM' so far. 'indexDSM' applies stronger filters and results
    are thus less representative.
    :return: Returns electric demand from driving filtered and aggregated to one fleet.
    """

    consumptionPower = scalars.loc['Electric consumption NEFZ', 'value']
    consumptionFuel = scalars.loc['Fuel consumption NEFZ', 'value']
    indexCons = filterCons.loc[:, 'indexCons']
    indexDSM = filterCons.loc[:, 'indexDSM']
    nHours = scalarsProc['noHours']
    electricPowerProfiles = consumptionProfiles.copy()
    for iHour in range(nHours):
        electricPowerProfiles[str(iHour)] = (consumptionProfiles[str(iHour)] - driveProfilesFuelAux[str(iHour)] *
                                           (consumptionPower / consumptionFuel))
        if filterIndex == 'indexCons':
            electricPowerProfiles[str(iHour)] = electricPowerProfiles[str(iHour)] * indexCons
        elif filterIndex == 'indexDSM':
            electricPowerProfiles[str(iHour)] = electricPowerProfiles[str(iHour)] * indexDSM
    return electricPowerProfiles


@logit
def setUnconsideredBatProfiles(chargeMaxProfiles, chargeMinProfiles, filterCons, minValue, maxValue):
    """
    Sets all profile values with filterCons = False to extreme values. For SoC max profiles, this means a value
    that is way higher than SoC max capacity. For SoC min this means usually 0. This setting is important for the
    next step of filtering out extreme values.

    :param chargeMaxProfiles: Dataframe containing hourly maximum SOC profiles for all profiles
    :param chargeMinProfiles: Dataframe containing hourly minimum SOC profiles for all profiles
    :param filterCons: Dataframe containing one boolean value for each profile
    :param minValue: Value that non-reasonable values of SoC min profiles should be set to.
    :param maxValue: Value that non-reasonable values of SoC max profiles should be set to.
    :return: Writes the two profiles files 'chargeMaxProfilesDSM' and 'chargeMinProfilesDSM' to the DataManager.
    """

    chargeMinProfilesDSM = chargeMinProfiles.copy()
    chargeMaxProfilesDSM = chargeMaxProfiles.copy()
    # if len(chargeMaxProfilesCons) == len(filterCons): #len(chargeMaxProfilesCons) = len(chargeMinProfilesCons) by design
    # How can I catch pandas.core.indexing.IndexingError ?
    try:
        chargeMinProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = minValue
        chargeMaxProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = maxValue
    except Exception as E:
        print("Declaration doesn't work. "
              "Maybe the length of filterCons differs from the length of chargeMaxProfiles")
        raise E
        # raise user defined

    return chargeMaxProfilesDSM, chargeMinProfilesDSM


@logit
def indexFilter(chargeMaxProfiles, chargeMinProfiles, filterCons):
    """
    Filters out profiles where indexCons is False.

    :param profiles: Profile keys in DataManager given as list of strings.
    :return: Writes filtered profiles to DataManager under the key 'profilesCons' in a dictionary with keys given
    by parameter profiles.
    """

    profilesFilterConsMin = chargeMinProfiles.loc[filterCons['indexCons'], :]
    profilesFilterConsMax = chargeMaxProfiles.loc[filterCons['indexCons'], :]
    profilesFilterDSMMin = chargeMinProfiles.loc[filterCons['indexDSM'], :]
    profilesFilterDSMMax = chargeMinProfiles.loc[filterCons['indexDSM'], :]
    return profilesFilterConsMin, profilesFilterConsMax, profilesFilterDSMMin, profilesFilterDSMMax


@logit
def socProfileSelection(profilesMin, profilesMax, filter, alpha):
    """
    Selects the nth highest value for each hour for min (max profiles based on the percentage given in parameter
    'alpha'. If alpha = 10, the 10%-biggest (10%-smallest) value is selected, all other values are disregarded.
    Currently, in the Venco reproduction phase, the hourly values are selected independently of each other. min and max
    profiles have to have the same number of columns.

    :param profilesMin: Profiles giving minimum hypothetic SOC values to supply the driving demand at each hour
    :param profilesMax: Profiles giving maximum hypothetic SOC values if vehicle is charged as soon as possible
    :param filter: Filter method. Currently implemented: 'singleValue'
    :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be fulfilled after selection.
    :return: Returns the two profiles 'SOCMax' and 'SOCMin' in the same time resolution as input profiles.
    """

    noProfiles = len(profilesMin)
    noProfilesFilter = int(alpha / 100 * noProfiles)
    if filter == 'singleValue':
        profileMinOut = profilesMin.iloc[0, :].copy()
        for col in profilesMin:
            profileMinOut[col] = min(profilesMin[col].nlargest(noProfilesFilter))

        profileMaxOut = profilesMax.iloc[0, :].copy()
        for col in profilesMax:
            profileMaxOut[col] = max(profilesMax[col].nsmallest(noProfilesFilter))

    else:
        # review have you considered implementing your own error like class FilterError(Exception):
        # pass which would give the user an additional hint on what went wrong?
        raise ValueError('You selected a filter method that is not implemented.')
    return profileMinOut, profileMaxOut


@logit
def normalizeProfiles(scalars, socMin, socMax, normReferenceParam):
    # ToDo: Implement a normalization to the maximum of a given profile

    """
    Normalizes given profiles with a given scalar reference.

    :param scalars: Dataframe containing technical assumptions e.g. battery capacity
    :param socMin: Minimum SOC profile subject to normalization
    :param socMax: Minimum SOC profile subject to normalization
    :param normReferenceParam: Reference parameter that is taken for normalization.
    This has to be given in scalar input data and is most likely the battery capacity.
    :return: Writes the normalized profiles to the DataManager under the specified keys
    """

    normReference = scalars.loc[normReferenceParam, 'value']
    try:
        socMinNorm = socMin.div(float(normReference))
        socMaxNorm = socMax.div(float(normReference))

    except ValueError:
        # review general if " is used instead of ' the escaping of \' is not necessary
        # review general so is this not a problem at all if this happens?
        # s I understand this code, socMin and socMax would be unchanged by this function call
        print('There was a value error. I don\'t know what to tell you.')
    return socMinNorm, socMaxNorm


@logit
def filterConsProfiles(profile, filterCons, critCol):
    """
    Filter out all profiles from given profile types whose boolean indices (so far DSM or cons) are FALSE.

    :param profile: Dataframe of hourly values for all filtered profiles
    :param filterCons: Identifiers given as list of string to store filtered profiles back into the DataManager
    :param critCol: Criterium column for filtering
    :return: Stores filtered profiles in the DataManager under keys given in dmgrNames
    """

    outputProfile = profile.loc[filterCons[critCol], :]
    return outputProfile


# FIXME so far not used. Plug profiles are aggregated in the action aggregateProfiles.
@logit
def considerProfiles(profiles, consider, colStart, colEnd, colCons):
    profilesOut = profiles.copy()

    try:
            profilesOut = profiles[consider[colCons].astype('bool'), colStart: colEnd]
    except KeyError:
        # review general: these are silent fails. How should the user react? Can this create a data problem
        # downstream? Is this invalidating your results or is this nothing to bother at all?
        # It is not clear from the error message. Also key is a bit unclear in this context.
        print("Key Error. "
            "The key {} is not part of {}".format(colCons, consider))
    return profilesOut


@logit
def aggregateProfiles(profilesIn):
    """
    This method aggregates all single-vehicle profiles that are considered to one fleet profile.

    :param profilesIn: Dataframe of hourly values of all filtered profiles
    :return: Returns a Dataframe with hourly values for one aggregated profile
    """

    # Typecasting is necessary for aggregation of boolean profiles
    profilesOut = profilesIn.iloc[0, :].astype('float64', copy=True)
    lenProfiles = len(profilesIn)

    # review have you considered using pandas dataframe .T to transpose,
    # use sum to get the sum of each column and then divide by lenProfiles?
    # This would be more concise in writing and more performant than a python loop
    for colidx in profilesIn:
        profilesOut[colidx] = sum(profilesIn.loc[:, colidx]) / lenProfiles
    return profilesOut


@logit
def correctProfiles(scalars, profile, profType):
    """
    This method scales given profiles by a correction factor. It was written for VencoPy scaling consumption data
    with the more realistic ARTEMIS driving cycle.

    :param scalars: Dataframe of technical assumptions
    :param profile: Dataframe of profile that should be corrected
    :param profType: A list of strings specifying if the given profile type is an electric or a fuel profile.
    profType has to have the same length as profiles.
    :return:
    """

    profileOut = profile.copy()
    if profType == 'electric':
        consumptionElectricNEFZ = scalars.loc['Electric consumption NEFZ', 'value']
        consumptionElectricArtemis = scalars.loc['Electric consumption Artemis', 'value']
        corrFactor = consumptionElectricArtemis / consumptionElectricNEFZ

    elif profType == 'fuel':
        consumptionFuelNEFZ = scalars.loc['Fuel consumption NEFZ', 'value']
        consumptionFuelArtemis = scalars.loc['Fuel consumption Artemis', 'value']
        corrFactor = consumptionFuelArtemis / consumptionFuelNEFZ

    else:
        # review I expect raising an exception here. Would it not be a problem if the processing continues silently?
        print('Either parameter "profType" is not given or not assigned to either "electric" or "fuel".')

    # review same like above:
    # review have you considered using pandas dataframe .T to transpose, use sum to get the sum of each column and
    # then divide by lenProfiles? This would be more concise in writing and more performant than a python loop
    for colIdx in profile.index:
        profileOut[colIdx] = corrFactor * profile[colIdx]
    return profileOut
