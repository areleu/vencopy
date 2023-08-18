__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod


import pandas as pd

from vencopy.core.dataParsers.dataParsers import IntermediateParsing
from vencopy.core.dataParsers.parkInference import ParkInference


class ParseMiD(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, load_encrypted=False, debug=False):
        """
        Class for parsing MiD data sets. The VencoPy configs globalConfig,
        parseConfig and localPathConfig have to be given on instantiation as
        well as the data set ID, e.g. 'MiD2017' that is used as key in the
        config lookups. Also, an option can be specified to load the file from
        an encrypted ZIP-file. For this, a password has to be given in the
        parseConfig.

        :param configDict: VencoPy config dictionary consisting at least of the
                           config dictionaries globalConfig, parseConfig and
                           localPathConfig.
        :param datasetID: A string identifying the MiD data set.
        :param load_encrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in parseConfig['PW'].
        """
        super().__init__(
            configDict=configDict,
            datasetID=datasetID,
            load_encrypted=load_encrypted,
            debug=debug,
        )
        self.park_inference = ParkInference(configDict=configDict)

    def __harmonize_variables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names
        given as specified in the mapping in parseConfig['dataVariables'].
        So far mappings for MiD08 and MiD17 are given. Since the MiD08 does
        not provide a combined household and person unique identifier, it is
        synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._create_replacement_dict(self.datasetID, self.dev_config["dataParsers"]["dataVariables"])
        activitiesRenamed = self.trips.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            activitiesRenamed["hhPersonID"] = (
                activitiesRenamed["hhID"].astype("string") + activitiesRenamed["personID"].astype("string")
            ).astype("int")
        self.trips = activitiesRenamed
        print("Finished harmonization of variables.")

    def __add_string_columns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        if weekday:
            self._add_string_column_from_variable(colName="weekdayStr", varName="tripStartWeekday")
        if purpose:
            self._add_string_column_from_variable(colName="purposeStr", varName="tripPurpose")

    def _drop_redundant_cols(self):
        # Clean-up of temporary redundant columns
        self.trips.drop(
            columns=[
                "isMIVDriver",
                "tripStartClock",
                "tripEndClock",
                "tripStartYear",
                "tripStartMonth",
                "tripStartWeek",
                "tripStartHour",
                "tripStartMinute",
                "tripEndHour",
                "tripEndMinute",
                "uniqueID_prev",
                "uniqueID_next",
                "colFromIndex",
            ],
            inplace=True,
        )

    def process(self) -> pd.DataFrame:
        """
        Wrapper function for harmonising and filtering the trips dataset as well as adding parking rows.

        :param split_overnight_trips: Should trips that end on the consecutive day (not the survey day) be split in such
        a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        self._select_columns()
        self.__harmonize_variables()
        self._harmonize_variables_unique_id_names()
        self._convert_types()
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self._update_end_timestamp()
        self._check_filter_dict()
        self._filter(self.filters)
        self._filter_consistent_hours()
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._cleanup_dataset()
        self.write_output()
        print("Parsing MiD dataset completed.")
        return self.activities

