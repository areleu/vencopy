__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '03.11.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from scripts.libInput import *
from scripts.libPreprocessing import *
from scripts.libProfileCalculation import *
from scripts.libOutput import *
# from scripts.libPlotting import *
from scripts.libLogging import logger
import pathlib
from profilehooks import profile

#ToDo: Maybe consolidate selection actions to one aggregation and one filtering action


@profile(immediate=True)
def vencoRun(config, dataset='MiD17'):
    #----- data and config read-in -----
    linkDict, scalars, driveProfilesRaw, plugProfilesRaw = readVencoInput(config, dataset)
    outputConfig = yaml.load(open(linkDict['linkOutputConfig']), Loader=yaml.SafeLoader)
    # indices = ['VEHICLE', 'Day', 'Weight']  # ['HP_ID_Reg', 'ST_WOTAG_str'] ['CASEID', 'PKWID']
    driveProfiles, plugProfiles = indexDriveAndPlugData(driveProfilesRaw, plugProfilesRaw, config['numberOfHours'])
    scalarsProc = procScalars(driveProfilesRaw, plugProfilesRaw, driveProfiles, plugProfiles)

    # driveProfiles = driveProfiles.query("ST_WOTAG_str == 'SAT'")
    # plugProfiles = plugProfiles.query("ST_WOTAG_str == 'SAT'")

    consumptionProfiles = calcConsumptionProfiles(driveProfiles, scalars)

    chargeProfiles = calcChargeProfiles(plugProfiles, scalars)

    chargeMaxProfiles = calcChargeMaxProfiles(chargeProfiles,
                                              consumptionProfiles,
                                              scalars,
                                              scalarsProc,
                                              nIter=7)

    chargeProfilesUncontrolled = calcChargeProfilesUncontrolled(chargeMaxProfiles,
                                                                scalarsProc)

    driveProfilesFuelAux = calcDriveProfilesFuelAux(chargeMaxProfiles,
                                                    chargeProfilesUncontrolled,
                                                    driveProfiles,
                                                    scalars,
                                                    scalarsProc)

    chargeMinProfiles = calcChargeMinProfiles(chargeProfiles,
                                              consumptionProfiles,
                                              driveProfilesFuelAux,
                                              scalars,
                                              scalarsProc,
                                              nIter=3)

    randNoPerProfile = createRandNo(driveProfiles)

    profileSelectors = calcProfileSelectors(chargeProfiles,
                                            consumptionProfiles,
                                            driveProfiles,
                                            driveProfilesFuelAux,
                                            randNoPerProfile,
                                            scalars,
                                            fuelDriveTolerance=1,
                                            isBEV=True)

    # Additional fuel consumption is subtracted from the consumption
    electricPowerProfiles = calcElectricPowerProfiles(consumptionProfiles,
                                                      driveProfilesFuelAux,
                                                      scalars,
                                                      profileSelectors,
                                                      scalarsProc,
                                                      filterIndex='indexDSM')

    # Profile filtering for flow profiles
    plugProfilesCons = filterConsProfiles(plugProfiles, profileSelectors, critCol='indexCons')
    electricPowerProfilesCons = filterConsProfiles(electricPowerProfiles, profileSelectors, critCol='indexCons')
    chargeProfilesUncontrolledCons = filterConsProfiles(chargeProfilesUncontrolled, profileSelectors,
                                                        critCol='indexCons')
    driveProfilesFuelAuxCons = filterConsProfiles(driveProfilesFuelAux, profileSelectors, critCol='indexCons')

    # Profile filtering for state profiles
    profilesSOCMinCons = filterConsProfiles(chargeMinProfiles, profileSelectors, critCol='indexDSM')
    profilesSOCMaxCons = filterConsProfiles(chargeMaxProfiles, profileSelectors, critCol='indexDSM')

    # Profile aggregation for flow profiles by averaging
    plugProfilesAgg = aggregateProfiles(plugProfilesCons)
    electricPowerProfilesAgg = aggregateProfiles(electricPowerProfilesCons)
    chargeProfilesUncontrolledAgg = aggregateProfiles(chargeProfilesUncontrolledCons)
    driveProfilesFuelAuxAgg = aggregateProfiles(driveProfilesFuelAuxCons)

    # Profile aggregation for state profiles by selecting one profiles value for each hour
    SOCMin, SOCMax = socProfileSelection(profilesSOCMinCons, profilesSOCMaxCons,
                                         filter='singleValue', alpha=1)

    # Profile correction for flow profiles
    chargeProfilesUncontrolledCorr = correctProfiles(scalars, chargeProfilesUncontrolledAgg, 'electric')
    electricPowerProfilesCorr = correctProfiles(scalars, electricPowerProfilesAgg, 'electric')
    driveProfilesFuelAuxCorr = correctProfiles(scalars, driveProfilesFuelAuxAgg, 'fuel')

    # Profile normalization for state profiles with the basis battery capacity
    socMinNorm, socMaxNorm = normalizeProfiles(scalars, SOCMin, SOCMax,
                                               normReferenceParam='Battery capacity')

    # Result output
    profileDictOut = dict(uncontrolledCharging=chargeProfilesUncontrolledCorr,
                          electricityDemandDriving=electricPowerProfilesCorr, SOCMax=socMaxNorm, SOCMin=socMinNorm,
                          gridConnectionShare=plugProfilesAgg, auxFuelDriveProfile=driveProfilesFuelAuxCorr)

    writeProfilesToCSV(profileDictOut=profileDictOut,
                       config=config,
                       singleFile=True,
                       dataset=dataset)

    # writeAnnualOutputForREMix(profileDictOut, outputConfig, linkDict['linkOutputAnnual'],
    #                         config['postprocessing']['hoursClone'], config['labels']['technologyLabel'],
    #                         strAdd='_MR1_alpha1_batCap40_cons15')

    linePlot(profileDictOut, linkOutput=linkDict['linkPlots'], config=config,
             show=True, write=True, filename='allPlots' + dataset)

    # Separately plot flow and state profiles
    profileDictConnectionShare = dict(gridConnectionShare=plugProfilesAgg)

    profileDictFlowsNorm = dict(uncontrolledCharging=chargeProfilesUncontrolledCorr,
                          electricityDemandDriving=electricPowerProfilesCorr, gridConnectionShare=plugProfilesAgg)
    profileDictFlowsAbs = dict(uncontrolledCharging=chargeProfilesUncontrolledAgg,
                                electricityDemandDriving=electricPowerProfilesAgg)

    profileDictStateNorm = dict(SOCMax=socMaxNorm, SOCMin=socMinNorm)
    profileDictStateAbs = dict(SOCMax=SOCMax, SOCMin=SOCMin)

    profileDictList = [profileDictConnectionShare, profileDictFlowsAbs, profileDictStateAbs]

    separateLinePlots(profileDictList, config,
                        show=True, write=True,
                        ylabel=['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh'],
                        filenames=[dataset + '_connection', dataset + '_flows', dataset + '_state'],
                        ylim=[1, 0.9, 50])


if __name__ == '__main__':
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    vencoRun(config=config, dataset='MiD08')
