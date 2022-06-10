__version__ = "0.1.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli, Parth Butte"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.09.2020"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd
import seaborn as sns
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from vencopy.utils.globalFunctions import (
    createFileString,
    calculateWeightedAverage,
    mergeDataToWeightsAndDays,
    writeProfilesToCSV,
)

class Evaluator:
    def __init__(
        self, configDict: dict, parseData: pd.Series = None, weightPlot=True
    ):
        """
        CURRENTLY IN SEMI-PROUDCTION MODE. Some interfaces may only apply to specific cases.
        Overall evaluation class for assessing vencopy mobility and charging profiles.

        :param configDict: A dictionary containing multiple yaml config files
        :param parseData: Series with instances of VencoPy class ParseData and keys specifying the name of the
            respective class
        :param weightPlot: If True, profiles are weighted for plotting
        """

        self.globalConfig = configDict["globalConfig"]
        self.evaluatorConfig = configDict["evaluatorConfig"]
        self.localPathConfig = configDict["localPathConfig"]
        self.weightPlot = weightPlot
        self.normPlotting = True
        self.dailyMileageGermany2008 = 3.080e9  # pkm/d
        self.dailyMileageGermany2017 = 3.214e9  # pkm/d
        self.hourVec = [
            str(i) for i in range(0, self.globalConfig["numberOfHours"])
        ]
        if parseData is not None:
            self.datasetIDs = list(parseData.index)
            self.parseData = parseData
            self.inputDataRaw = self.readInData(
                ["inputDataDriveProfiles"], self.datasetIDs
            )
            self.inputData = pd.Series(dtype=object)
            self.mergeDaysAndWeights()
            self.data = pd.Series(dtype=object)
            self.reindexData()
            self.dataStacked = None
            self.aggregateIDDict = self.setupAggDict()
            self.hourlyAggregates = self.aggregateAcrossTrips()

        print("Evaluator initialization complete")

    def readInData(self, fileKeys: list, datasets: list) -> pd.Series:
        """
        Generic read-in function for mobility datasetIDs. This serves as interface between the daily trip distance
        and purpose calculation and the class Evaluator.

        :param fileKeys: List of VencoPy-internal names for the filekeys to read in
        :param datasets: list of strings declaring the datasetIDs to be read in
        :return: a named pd.Series of all datasetIDs with the given filekey_datasets as identifiers
        """

        ret = pd.Series(dtype=object)
        for iFileKey in fileKeys:
            for iDat in datasets:
                dataIn = pd.read_csv(
                    Path(self.localPathConfig["pathAbsolute"]["vencoPyRoot"])
                    / (self.globalConfig["pathRelative"]["diaryOutput"])
                    / createFileString(
                        globalConfig=self.globalConfig,
                        fileKey=iFileKey,
                        datasetID=iDat,
                    ),
                    dtype={"genericID": int},
                    # index_col=['genericID', 'tripStartWeekday'])
                    index_col=["genericID"],
                )
                ret[iDat] = dataIn
        return ret

    def mergeDaysAndWeights(self):
        """
        Method to merge mobility data to respective trip weights and days.

        :return: None
        """
        for iDat in self.datasetIDs:
            self.inputData[iDat] = mergeDataToWeightsAndDays(
                self.inputDataRaw[iDat], self.parseData[iDat]
            )
            self.inputData[iDat].dropna()

    # DEPRECATED WILL BE DELETED ON NEXT RELEASE
    def assignWeight(self, datasetIDs: list):
        """
        Reformatting function

        :param datasetIDs:
        :return:
        """

        ret = pd.Series(dtype=object)
        for iDat in datasetIDs:
            weightData = mergeDataToWeightsAndDays(
                self.inputData[iDat], self.parseData[iDat]
            )
            weights = weightData.loc[
                :, ["genericID", "tripStartWeekday", "tripWeight"]
            ]
            weights = weights.convert_dtypes()
            ret[iDat] = weights.set_index(["genericID"], drop=True)
        return ret

    def calculateMobilityQuota(self, dataset: str) -> None:
        """
        Calculates the number of survey days where mobiity occured.

        :param dataset: name of dataset
        :return: Scalar, the ratio of mobile days to total days
        """
        dataKey = f"inputDataDriveProfiles_{dataset}"
        if dataKey not in self.inputData.keys():
            assert "Specified dataset was not read in during Evaluator initialization"
        dat = self.inputData[dataKey]
        isNoTrip = dat == 0
        isNoTrip.apply(any, axis=1)

    def reindexData(self):
        """
        Formatting function to set index of profiles with weekday and weight,
        in preparation for plotting

        :return: None
        """
        for iDat in self.datasetIDs:
            self.data[iDat] = self.inputData[iDat].set_index(
                ["genericID", "tripStartWeekday"], drop=True
            )
            self.data[iDat].dropna(inplace=True)

    # Maybe not needed at all?
    def stackData(self):
        """
        Function to rearrange and rename data for plotting

        :return: None
        """
        ret = {}
        for idx, iDat in self.inputData.items():
            iDatRaw = (
                iDat.drop(columns=["genericID"])
                .set_index("tripStartWeekday", append=True)
                .stack()
            )  # 'tripWeight', 'tripScaleFactor'
            iDat = iDatRaw.reset_index([1, 2])
            iDat.columns = ["Day", "Hour", "Value"]
            ret[idx] = iDat
        return ret

    def setupAggDict(self):
        """
        Setup lookup-dictionaries for plotting.

        :return: Updated dictionary of data set IDs
        """
        IDDict = {"sum": None, "mean": None, "wMean": None}
        IDDict["sum"] = [f"{iDatID}_sum" for iDatID in self.datasetIDs]
        IDDict["mean"] = [f"{iDatID}_mean" for iDatID in self.datasetIDs]
        IDDict["wMean"] = [f"{iDatID}_wMean" for iDatID in self.datasetIDs]
        return IDDict

    def aggregateAcrossTrips(self):
        """
        Aggregate trip distances across all trops for specific hours. Aggregation is carried out threefold: Summation,
        simple average and weighted average if self.weightPlot is True.

        :return: DataFrame with a column for each aggregated time series and a row for each hour in the original data
            set
        """
        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            ret.loc[:, f"{iDatID}_sum"] = iDat.loc[:, self.hourVec].sum(axis=0)
            ret.loc[:, f"{iDatID}_mean"] = iDat.loc[:, self.hourVec].mean(
                axis=0
            )
            if self.weightPlot:
                ret.loc[:, f"{iDatID}_wMean"] = iDat.loc[
                    :, self.hourVec
                ].apply(
                    calculateWeightedAverage, args=[iDat.loc[:, "tripWeight"]]
                )
        return ret

    def calcVariableSpecAggregates(self, by: list):
        """
        Aggregation method that calculated aggregates such in aggregateAcrossTrips but differentiating one additional
        criterium given in by. The method is used to calculate weekday specific aggregates.

        :param by: List of column names to add differentiation. Currently only tested for tripStartWeekday
        :return: Pandas DataFrame with three columns each one for summed, average and weighted average and a multiindex
            differentiating between hours and one additional variable specificied in by.
        """
        ret = pd.DataFrame()
        for iDatID, iDat in self.data.items():
            if not all([iBy in iDat.index.names for iBy in by]):
                raise Exception(
                    "At least one required variable name is not in index names. Aborting"
                )
            ret.loc[:, f"{iDatID}_sum"] = (
                iDat.loc[:, self.hourVec].groupby(level=by).sum().stack()
            )  # needed .stack() here?
            ret.loc[:, f"{iDatID}_mean"] = (
                iDat.loc[:, self.hourVec].groupby(level=by).mean().stack()
            )
            if self.weightPlot:
                # ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:,
                #                                 self.hourVec].groupby(level=by).apply(calculateWeightedAverage,
                #                                                     weightCol=[iDat.loc[:, 'tripWeight']]).stack()
                ret.loc[:, f"{iDatID}_wMean"] = self.calcWeightedTripValues(
                    dataIn=iDat, idxLvl=by[0]
                )
        return ret

    def calcWeightedTripValues(self, dataIn, idxLvl):
        """
        Function to calculate weighted trip values with an additional differentiating column given in idxLvl.

        :param dataIn: Input data for aggregation
        :param idxLvl: Index level specifying additional differentiation criterium
        :return:
        """
        vars = set(dataIn.index.get_level_values(idxLvl))
        ret = pd.DataFrame(index=self.hourVec, columns=vars)
        data = dataIn.loc[:, self.hourVec].reset_index(level=idxLvl)
        weights = dataIn.loc[:, "tripWeight"].reset_index(level=idxLvl)
        for iVar in vars:
            dataSlice = data.loc[data.loc[:, idxLvl] == iVar, self.hourVec]
            weightSlice = weights.loc[
                data.loc[:, idxLvl] == iVar, "tripWeight"
            ]
            ret.loc[:, iVar] = dataSlice.apply(
                calculateWeightedAverage, weightCol=weightSlice
            )
        ret = ret.stack()
        ret.index = ret.index.swaplevel(0, 1)
        return ret

    def sortData(self, data):
        """
        Method used for plotting to order index values

        :param data: Pandas Dataframe with two indices
        """
        data.index = data.index.swaplevel(0, 1)
        return data.sort_index()

    def plotAggregates(self):
        """
        Plotting method for plotting aggregates that are differentiated with one additional variable. In the current
        implementation both summed and averaged values are plotted. Arguments show and write in the evaluator config
        determine if plots are shown to the user and/or written to hard disk. X axis ticks are currently manually
        configured to fit weekday and hour differentiation for one week.

        :return: None
        """
        self.hourlyAggregates = self.hourlyAggregates.swaplevel(0, 1)
        # Plotting aggregates
        fig, ax = plt.subplots(2, 1)
        plt.tick_params(
            labelsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "font.size"
            ]
        )
        meanCols = self.aggregateIDDict["mean"]
        meanCols.extend(self.aggregateIDDict["wMean"])
        self.hourlyAggregates.loc[:, self.aggregateIDDict["sum"]].plot.line(
            ax=ax[0]
        )
        self.hourlyAggregates.loc[:, meanCols].plot.line(ax=ax[1])
        xRange = range(
            0,
            len(self.hourlyAggregates) + 1,
            self.evaluatorConfig["plotConfig"]["xAxis"]["xTickSteps"],
        )
        xLabels = [
            f"{iDay}\n{str(iTime)}:00"
            for iDay in self.evaluatorConfig["plotConfig"]["xAxis"]["weekdays"]
            for iTime in self.evaluatorConfig["plotConfig"]["xAxis"]["hours"]
        ]
        # xLabels = [f'{str(iTime)}:00' for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        ax[1].set_xticks(xRange[:-1])
        ax[1].set_xticklabels(
            xLabels,
            fontsize=self.evaluatorConfig["plotConfig"]["xAxis"][
                "ticklabelsize"
            ],
        )
        ax[0].ticklabel_format(
            axis="y", style="sci", scilimits=(0, 0), useMathText=True
        )
        ax[0].set_xticks(xRange[:-1])
        ax[0].set_xticklabels("")
        ax[0].set_xlabel("")
        ax[0].set_ylabel(
            "Sum of all trips \n in sample in km",
            fontsize=self.evaluatorConfig["plotConfig"]["yAxis"][
                "ticklabelsize"
            ],
        )
        ax[1].set_xlabel(
            "Hour",
            fontsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "axes.labelsize"
            ],
        )
        ax[1].set_ylabel(
            "Average of hourly \n trips in km",
            fontsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "axes.labelsize"
            ],
        )
        ax[0].tick_params(
            axis="y",
            labelsize=self.evaluatorConfig["plotConfig"]["yAxis"][
                "ticklabelsize"
            ],
        )
        ax[0].legend()
        ax[1].legend()

        if self.evaluatorConfig["plotConfig"]["show"]:
            plt.show()
        if self.evaluatorConfig["plotConfig"]["save"]:
            fileName = createFileString(
                globalConfig=self.globalConfig,
                fileKey="aggPlotName",
                manualLabel=self.globalConfig["labels"]["runLabel"],
                filetypeStr="svg",
            )
            fig.savefig(
                Path(self.localPathConfig["pathAbsolute"]["vencoPyRoot"])
                / self.globalConfig["pathRelative"]["evalOutput"]
                / fileName,
                bbox_inches="tight",
            )

    def linePlot(
        self,
        profileDict,
        pathOutput,
        flexEstimator,
        show=True,
        write=True,
        ylabel="Normalized profiles",
        ylim=None,
        filename="",
    ):
        """
        Basic line plot functionality

        :param profileDict: Dictionary specifiying which profiles should be plotted together in one plot
        :param pathOutput: Path to write Figure to
        :param flexEstimator: Instance of VencoPy class FlexEstimator for plotting of charging profiles
        :param show: If True, Figure is displayed during runtime
        :param write: If True, Figrue is written to hard disk
        :param ylabel: Label for y Axis
        :param ylim: Manually set y limit
        :param filename: Manually set name of file written to hard disk
        :return: None
        """
        plt.rcParams.update(
            self.evaluatorConfig["plotConfig"]["plotRCParameters"]
        )  # set plot layout
        fig, ax = plt.subplots()
        plt.tick_params(
            labelsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "font.size"
            ]
        )
        for iKey, iVal in profileDict.items():
            if isinstance(iVal.index, pd.MultiIndex):
                iVal = self.sortData(iVal)
                sns.lineplot(
                    x=range(iVal.index.size), y=iVal, label=iKey, sort=False
                )
            else:
                sns.lineplot(x=iVal.index, y=iVal, label=iKey, sort=False)
        xRange = range(
            0,
            len(profileDict[list(profileDict)[0]]) + 1,
            self.evaluatorConfig["plotConfig"]["xAxis"]["xTickSteps"],
        )
        xLabels = [
            f"{iDay}\n{str(iTime)}:00"
            for iDay in self.evaluatorConfig["plotConfig"]["xAxis"]["weekdays"]
            for iTime in self.evaluatorConfig["plotConfig"]["xAxis"]["hours"]
        ]

        # Labeling for 24 hour plots
        # xLabels = [f'{str(iTime)}:00' for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        ax.set_xticks(xRange[:-1])
        ax.set_xticklabels(
            xLabels,
            fontsize=self.evaluatorConfig["plotConfig"]["xAxis"][
                "ticklabelsize"
            ],
        )
        if ylim:
            ax.set_ylim(bottom=0, top=ylim)
        ax.set_xlabel(
            "Weekday and Hour",
            fontsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "axes.labelsize"
            ],
        )
        ax.set_ylabel(
            ylabel,
            fontsize=self.evaluatorConfig["plotConfig"]["plotRCParameters"][
                "axes.labelsize"
            ],
        )
        plt.legend(loc="upper center")
        plt.tight_layout()
        filePlot = pathOutput / Path(
            createFileString(
                globalConfig=self.globalConfig,
                datasetID=flexEstimator.datasetID,
                fileKey="flexPlotName",
                manualLabel=filename,
                filetypeStr="svg",
            )
        )
        if show:
            plt.show()
        if write:
            fig.savefig(filePlot)

    def separateLinePlots(
        self,
        profileDictList: list,
        flexEstimator,
        show=True,
        write=True,
        ylabel=[],
        ylim=[],
        filenames=[],
    ):
        """
        Wrapper function to draw and write multiple plots using linePlot().

        :param profileDictList: List of dictionaries. Each dictionary specifies one plot drawn.
        :param flexEstimator: Instance of VencoPy class FlexEstimator for data set ID
        :param show: If True, plots are shown to the user during runtime
        :param write: If True, plots are written to hard drive
        :param ylabel: List of ylabels. Has to be of same length as profileDictList
        :param ylim: List of ylimits. Has to be of same length as profileDictList
        :param filenames: Name of file to be written to hard drive
        :return: None
        """
        if ylim:
            for iDict, iYLabel, iYLim, iName in zip(
                profileDictList, ylabel, ylim, filenames
            ):
                writeProfilesToCSV(
                    profileDictOut=iDict,
                    globalConfig=self.globalConfig,
                    localPathConfig=self.localPathConfig,
                    singleFile=False,
                    datasetID=flexEstimator.datasetID,
                )
                self.linePlot(
                    iDict,
                    pathOutput=Path(
                        self.localPathConfig["pathAbsolute"]["vencoPyRoot"]
                    )
                    / self.globalConfig["pathRelative"]["evalOutput"],
                    flexEstimator=flexEstimator,
                    show=show,
                    write=write,
                    ylabel=iYLabel,
                    ylim=iYLim,
                    filename=iName,
                )
        else:
            for iDict, iYLabel, iName in zip(
                profileDictList, ylabel, filenames
            ):
                writeProfilesToCSV(
                    profileDictOut=iDict,
                    globalConfig=self.globalConfig,
                    localPathConfig=self.localPathConfig,
                    singleFile=False,
                    datasetID=flexEstimator.datasetID,
                )
                self.linePlot(
                    iDict,
                    pathOutput=Path(
                        self.localPathConfig["pathAbsolute"]["vencoPyRoot"]
                    )
                    / self.globalConfig["pathRelative"]["evalOutput"],
                    flexEstimator=flexEstimator,
                    show=show,
                    write=write,
                    ylabel=iYLabel,
                    filename=iName,
                )

    def plotProfiles(
        self,
        flexEstimator,
        profileDictList: dict = None,
        yLabels: list = None,
        yLimits: list = None,
        filenames: list = None,
    ):
        """
        Wrapper function to plot both one Figure with all resulting output profiles and separate Figures for flow,
        connection and state profiles after VencoPy flexibility estimation.

        :param flexEstimator: Instance of VencoPy class FlexEstimator
        :param profileDictList: List of diciontaries with keys specifying the profile name and value holding a pandas
        dataframe with a multiindex series
        :param yLabels: list of y axis labels. Has to be of same length as profileDictList
        :param yLimitsL List of y axis limits. Has to be of same length as profileDictList
        :param filenames: List of filenames to write the plots to. Has to be of same length as profileDictList
        :return: None
        """

        if profileDictList is None:
            # Separately plot flow and state profiles
            profileDictConnectionShare = dict(
                gridConnectionShare=flexEstimator.plugProfilesWAggVar
            )

            # profileDictFlowsNorm = dict(uncontrolledCharging=flexEstimator.chargeProfilesUncontrolledCorr,
            #                             electricityDemandDriving=flexEstimator.electricPowerProfilesCorr,
            #                             gridConnectionShare=flexEstimator.plugProfilesAgg)
            profileDictFlowsAbs = dict(
                uncontrolledCharging=flexEstimator.chargeProfilesUncontrolledWAggVar,
                electricityDemandDriving=flexEstimator.electricPowerProfilesWAggVar,
            )

            # profileDictStateNorm = dict(socMax=flexEstimator.socMaxNorm, socMin=flexEstimator.socMinNorm)
            profileDictStateAbs = dict(
                socMax=flexEstimator.socMaxVar, socMin=flexEstimator.socMinVar
            )

            profileDictList = [
                profileDictConnectionShare,
                profileDictFlowsAbs,
                profileDictStateAbs,
            ]

            yLabels = [
                "Average EV connection share between 0 and 1",
                "Average hourly electricity volume in kWh",
                "Average EV SOC in kWh",
            ]
            filenames = [
                flexEstimator.datasetID + "_connection",
                flexEstimator.datasetID + "_flows",
                flexEstimator.datasetID + "_state",
            ]
        self.separateLinePlots(
            profileDictList,
            show=True,
            write=True,
            flexEstimator=flexEstimator,
            ylabel=yLabels,
            filenames=filenames,
        )

    def compareProfiles(self, compareTo):
        """
        EXPERIMENTAL STATE. Deprecated method to compare profiles of MiD08 and MiD17. Currently not in production and
        not tested.
        """
        # if not isinstance(compareTo, FlexEstimator):
        #     raise('Argument to compare to is not a class instance of FlexEstimator')

        profileList = [
            # 'plugProfilesAgg', 'plugProfilesWAgg', 'chargeProfilesUncontrolledAgg',
            # 'chargeProfilesUncontrolledWAgg', 'electricPowerProfilesAgg', 'electricPowerProfilesWAgg',
            # 'plugProfilesWAggVar', 'electricPowerProfilesWAggVar', 'chargeProfilesUncontrolledWAggVar'
            # 'auxFuelDemandProfilesWAggVar',
        ]

        profileDictList = self.compileDictList(
            compareTo=compareTo, profileNameList=profileList
        )
        SOCDataWeek = {
            "MiD08_SOCmin": self.SOCMinVar,
            "MiD08_SOCmax": self.SOCMaxVar,
            "MiD17_SOCmin": compareTo.SOCMinVar,
            "MiD17_SOCmax": compareTo.SOCMaxVar,
        }

        profileDictList.append(SOCDataWeek)

        self.separateLinePlots(
            profileDictList,
            show=self.evaluatorConfig["plotConfig"]["show"],
            write=self.evaluatorConfig["plotConfig"]["save"],
            ylabel=[
                # 'Average EV connection share', 'Weighted Average EV connection share',
                # 'Uncontrolled charging in kW', 'Weighted Uncontrolled charging in kW',
                # 'Electricity consumption for driving in kWh',
                # 'Weighted Electricity consumption for driving in kWh',
                # 'Weighted average EV fleet connection share',
                # 'Electricity consumption for driving in kWh',
                # 'Weighted average uncontrolled charging in kW'
                # 'auxFuelDemandProfilesWAggVar'
                "State of charge in kWh"
            ],
            filenames=[
                # '_connection', '_connectionWeighted',
                # '_uncCharge', '_uncChargeWeighted',
                # '_drain', '_drainWeighted',
                # '_plugDiffDay', '_drainDiffDay',
                # '_uncChargeDiffDay'
                #  '_auxFuelDiffDay',
                "_socWeek"
            ],
            ylim=[
                # 1, 1, 1,
                # 1, 1, 1
                # 1, 1, 1
                # 1
                50
            ],
        )

    def compileDictList(self, compareTo, profileNameList):
        """
        Helper function for compareProfiles()

        :param compareTo: Dataset to compare to
        :param profileNameList: List of names of files to be written
        :return:
        """
        ret = []
        keys = [self.datasetID, compareTo.datasetID]
        for iProf in profileNameList:
            iDict = self.compileProfileComparisonDict(
                keys=keys,
                values=[getattr(self, iProf), getattr(compareTo, iProf)],
            )
            ret.append(iDict)
        return ret

    def compileProfileComparisonDict(self, keys: list, values: list):
        return {iKey: iVal for iKey, iVal in zip(keys, values)}

    def compareUncontrolledCharging(self, pathToFile: str, sheetname: str):
        dataIn = pd.read_excel(
            io=pathToFile, sheet_name=sheetname, header=1, engine="openpyxl"
        )
        data = dataIn.groupby(by=["day"]).sum().drop(columns="hour")
        # FIXME: ax = plt.subplots()
        data.plot()
        plt.show()
        print(data)

    def boxPlot(self, profiles):
        profiles.boxplot()
        plt.show()

    def plotParkingAndPowers(self, vpGrid, write=True):
        """
        Wrapper function to plot parking purpose and charging distribution
        """
        self.plotParkingPurpose(purposes=vpGrid.purposeData, write=write)
        self.plotParkingPower(charingPower=vpGrid.chargeAvailability, write=write)

    def plotParkingPurpose(self, purposes: pd.DataFrame, write: bool):
        """
        :param purposes: Dataframe holding hourly purposes
        """
        # Plot for purposes
        # purposeList = list(purposeDict)
        purposeList = list(np.unique(purposes.loc[:, :].values))
        # purposes = purposes.set_index(['genericID']).transpose()
        purposes = purposes.transpose()
        totalTripPurpose = pd.DataFrame()

        for i in range(0, len(purposeList)):
            total = purposes.where(purposes.loc[:] == purposeList[i]).count(
                axis=1
            )
            totalTripPurpose = pd.concat(
                [totalTripPurpose, total], ignore_index=True, axis=1
            )
        totalTripPurpose.columns = (
            totalTripPurpose.columns[: -len(purposeList)].tolist()
            + purposeList
        )
        totalTripPurpose.index = np.arange(1, len(totalTripPurpose) + 1)

        fig, ax = plt.subplots(1, figsize=(20, 8))
        x = np.arange(0, len(totalTripPurpose.index))
        plt.bar(
            x - 0.426,
            totalTripPurpose.iloc[:, 0],
            width=0.142,
            color="#D35400",
        )
        plt.bar(
            x - 0.284,
            totalTripPurpose.iloc[:, 1],
            width=0.142,
            color="#1D2F6F",
        )
        plt.bar(
            x - 0.142,
            totalTripPurpose.iloc[:, 2],
            width=0.142,
            color="#FA8390",
        )
        plt.bar(x, totalTripPurpose.iloc[:, 3], width=0.142, color="#FF0000")
        plt.bar(
            x + 0.142,
            totalTripPurpose.iloc[:, 4],
            width=0.142,
            color="#6EAF46",
        )
        plt.bar(
            x + 0.284,
            totalTripPurpose.iloc[:, 5],
            width=0.142,
            color="#FAC748",
        )
        plt.bar(
            x + 0.426,
            totalTripPurpose.iloc[:, 6],
            width=0.142,
            color="#928aed",
        )
        # plt.bar(x + 0.4375, totalTripPurpose['0.0'], width=0.142, color='#1ABC9C')
        plt.ylabel("Trip purposes during 24 hours")
        plt.xlabel("Time (hours)")
        plt.xticks(x, totalTripPurpose.index)
        plt.xlim(-1, 24)
        ax.yaxis.grid(color="black", linestyle="dashed", alpha=0.3)
        plt.legend(
            totalTripPurpose.columns, loc="upper center", ncol=len(purposeList)
        )
        plt.show()
        if write:
            plt.savefig(Path(self.localPathConfig['pathAbsolute']['vencoPyRoot']) /
                             self.globalConfig['pathRelative']['evalOutput'] /
                             createFileString(globalConfig=self.globalConfig,
                                            fileKey='vencoPyOutput',
                                            manualLabel='purposeDist',
                                            filetypeStr='png'),
                        format='png')

    def plotParkingPower(self, charingPower: pd.DataFrame, write: bool):
        """
        charingPower: Dataframe holding hourly charging station power
        """
        # Plot for charging station
        # capacity = pd.read_csv(self.outputFilePath, keep_default_na=False, index_col='genericID')
        capacity = charingPower.transpose()
        capacityList = list(np.unique(capacity.loc[:, :].values))

        totalChargingStation = pd.DataFrame()
        for i in range(0, len(capacityList)):
            total = capacity.where(capacity.loc[:] == capacityList[i]).count(
                axis=1
            )
            totalChargingStation = pd.concat(
                [totalChargingStation, total], ignore_index=True, axis=1
            )
        totalChargingStation.columns = (
            totalChargingStation.columns[: -len(capacityList)].tolist()
            + capacityList
        )
        totalChargingStation.index = np.arange(
            1, len(totalChargingStation) + 1
        )
        totalChargingStation.plot(
            kind="area",
            title="Vehicles connected to different charging station over 24 hours",
            figsize=(10, 8),
            colormap="Paired",
        )
        capacityListStr = [str(x) for x in capacityList]
        appendStr = " kW"
        capacityListStr = [sub + appendStr for sub in capacityListStr]
        plt.xlim(1, 24)
        plt.xlabel("Time (hours)")
        plt.ylabel("Number of vehicles")
        plt.legend(capacityListStr, loc="upper center", ncol=len(capacityList))
        plt.show()
        if write:
            plt.savefig(Path(self.localPathConfig['pathAbsolute']['vencoPyRoot']) /
                             self.globalConfig['pathRelative']['evalOutput'] /
                             createFileString(globalConfig=self.globalConfig,
                                            fileKey='vencoPyOutput',
                                            manualLabel='powerDist',
                                            filetypeStr='png'),
                        format='png')


class chargingTransactionEvaluator:
    def __init__(
        self, evaluatorConfig: dict, parseData, gridModeler, flexEstimator
    ):
        # globalConfig:dict, parseData: pd.Series = None,
        """
        Evaluator to specifically assess charging transaction data

        :param evaluatorConfig: Evaluator config mainly for plot properties
        :param parseData: Series with instances of VencoPy class ParseData and keys specifying the name of the
            respective class
        :param gridModeler: GridModeler instance for the analysis of charging transactions. The gridModeler holds both
            parkPurposeDiaries and plugProfiles
        :param flexEstimator: FlexEstimator instance providing results of the VencoPy run.
        """

        self.evaluatorConfig = evaluatorConfig
        self.parseData = parseData
        self.gridModeler = gridModeler
        self.flexEstimator = flexEstimator
        print("Charging transaction evaluator initialization complete")

    def createTransactionData(self):
        noTrans = self._countTransactions()
        df = pd.DataFrame()
        idxAll = pd.MultiIndex()
        for iR in noTrans.iterrows():
            idx = pd.MultiIndex.from_product(
                [[noTrans.index[iR]], range(noTrans.iloc[iR])]
            )
            idxAll.append(idx)
        print("blub")

    def _countTransactions(self):
        dfDiff = self.flexEstimator.chargeProfiles.diff(axis=1)
        sTrans = pd.Series(data=1, index=dfDiff.index)
        sTrans.loc[dfDiff.loc[:, 0] == 0, :] = 0
        return sTrans + dfDiff.gt(0).sum(axis=1)

    def plotPurposes(self):
        pass

    def plotChargingPower(self):
        pass


if __name__ == "__main__":
    from vencopy.core.dataParsers import DataParser
    from vencopy.utils.globalFunctions import loadConfigDict

    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "tripConfig",
        "gridConfig",
        "flexConfig",
        "evaluatorConfig",
    )
    configDict = loadConfigDict(configNames)

    parseDataAll = pd.Series(dtype=object)
    # parseDataAll['MiD08'] = DataParser(datasetID='MiD08', configDict=configDict, loadEncrypted=False)
    parseDataAll["MiD17"] = DataParser(
        datasetID="MiD17", configDict=configDict, loadEncrypted=False
    )

    vpEval = Evaluator(configDict=configDict, parseData=parseDataAll)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(
        by=["tripStartWeekday"]
    )
    vpEval.plotAggregates()

    # vpEval.data = mergeVariables(data=vpEval.inputData['MiD17'].reset_index(),
    #                              variableData=p.data, variables=['tripWeight'])
