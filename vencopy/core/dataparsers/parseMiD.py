__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd

from ...core.dataparsers.dataparsers import IntermediateParsing
from ...core.dataparsers.parkinference import ParkInference


class ParseMiD(IntermediateParsing):
    def __init__(self, configs: dict, dataset: str):
        """
        Class for parsing MiD data sets. The venco.py configs globalConfig,
        parseConfig and localPathConfig have to be given on instantiation as
        well as the data set ID, e.g. 'MiD2017' that is used as key in the
        config lookups. Also, an option can be specified to load the file from
        an encrypted ZIP-file. For this, a password has to be given in the
        parseConfig.

        :param configs: venco.py config dictionary consisting at least of the
                           config dictionaries globalConfig, parseConfig and
                           localPathConfig.
        :param dataset: A string identifying the MiD data set.
        :param load_encrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in parseConfig['PW'].
        """
        super().__init__(
            configs=configs,
            dataset=dataset
        )
        self.park_inference = ParkInference(configs=configs)

    def __harmonise_variables(self):
        """
        Harmonizes the input data variables to match internal venco.py names
        given as specified in the mapping in parseConfig['data_variables'].
        So far mappings for MiD08 and MiD17 are given. Since the MiD08 does
        not provide a combined household and person unique identifier, it is
        synthesized of the both IDs.

        :return: None
        """
        replacement_dict = self._create_replacement_dict(
            self.dataset, self.dev_config["dataparsers"]["data_variables"]
        )
        activities_renamed = self.trips.rename(columns=replacement_dict)
        if self.dataset == "MiD08":
            activities_renamed["household_person_id"] = (
                activities_renamed["household_id"].astype("string") + activities_renamed["person_id"].astype("string")
            ).astype("int")
        self.trips = activities_renamed
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
            self._add_string_column_from_variable(col_name="weekday_string", var_name="trip_start_weekday")
        if purpose:
            self._add_string_column_from_variable(col_name="purpose_string", var_name="trip_purpose")

    def _drop_redundant_columns(self):
        """
        Removes temporary redundant columns.
        """
        self.trips.drop(
            columns=[
                "is_driver",
                "trip_start_clock",
                "trip_end_clock",
                "trip_start_year",
                "trip_start_month",
                "trip_start_week",
                "trip_start_hour",
                "trip_start_minute",
                "trip_end_hour",
                "trip_end_minute",
                "previous_unique_id",
                "next_unique_id",
                "column_from_index",
            ],
            inplace=True,
        )

    def process(self) -> pd.DataFrame:
        """
        Wrapper function for harmonising and filtering the trips dataset as well as adding parking rows.
        """
        self._load_data()
        self._select_columns()
        self.__harmonise_variables()
        self._harmonize_variables_unique_id_names()
        self._convert_types()
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self._update_end_timestamp(trips=self.trips)
        self._check_filter_dict(dictionary=self.filters)
        self._filter(filters=self.filters)
        self._filter_consistent_hours(dataset==self.trips)
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._cleanup_dataset()
        self.write_output()
        print("Parsing MiD dataset completed.")
        return self.activities
