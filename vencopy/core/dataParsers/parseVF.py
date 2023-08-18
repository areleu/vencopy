__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod

import pandas as pd

from vencopy.core.dataParsers.dataParsers import IntermediateParsing
from vencopy.core.dataParsers.parkInference import ParkInference

class ParseVF(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, debug, load_encrypted=False):
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
        super().__init__(configDict=configDict, datasetID=datasetID, debug=debug, load_encrypted=load_encrypted)
        self.park_inference = ParkInference(configDict=configDict)

    def _load_data(self):
        """
        rawDataPathTrip, unlike for other MiD classes is taken from the MiD B1 dataset
        rawDataPathVehicles is an internal dataset from VF
        """
        rawDataPathTrips = (
            Path(self.user_config["global"]["pathAbsolute"][self.datasetID])
            / self.dev_config["global"]["files"][self.datasetID]["tripsDataRaw"]
        )
        rawDataPathVehicles = (
            Path(self.user_config["global"]["pathAbsolute"][self.datasetID])
            / self.dev_config["global"]["files"][self.datasetID]["vehiclesDataRaw"]
        )
        rawDataTrips = pd.read_stata(
            rawDataPathTrips,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        rawDataVehicles = pd.read_csv(rawDataPathVehicles, encoding="ISO-8859-1")
        rawDataVehicles = rawDataVehicles.drop(columns=["Unnamed: 0"])
        rawDataVehicles = rawDataVehicles.drop_duplicates(subset=["HP_ID"], keep="first")
        rawDataVehicles.set_index("HP_ID", inplace=True)
        raw_data = rawDataTrips.join(rawDataVehicles, on="HP_ID", rsuffix="VF")
        self.raw_data = raw_data
        print(f"Finished loading {len(self.raw_data)} rows of raw data of type .dta.")

    def __harmonize_variables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names given as specified in the mapping in
        self.dev_config["dataParsers"]['dataVariables']. Mappings for MiD08 and MiD17 are given. Since the MiD08 does not provide a
        combined household and person unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._create_replacement_dict(self.datasetID, self.dev_config["dataParsers"]["dataVariables"])
        dataRenamed = self.trips.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string") + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.trips = dataRenamed
        print("Finished harmonization of variables")

    def __pad_missing_car_segments(self):
        # remove vehicleSegment nicht zuzuordnen
        self.trips = self.trips[self.trips.vehicleSegment != "nicht zuzuordnen"]
        # pad missing car segments
        # self.trips.vehicleSegment = self.trips.groupby('hhID').vehicleSegment.transform('first')
        # self.trips.drivetrain = self.trips.groupby('hhID').drivetrain.transform('first')
        # self.trips.vehicleID = self.trips.groupby('hhID').vehicleID.transform('first')
        # remove remaining NaN
        self.trips = self.trips.dropna(subset=["vehicleSegment"])
        # self.trips = self.trips.dropna(subset=['vehicleSegment', 'drivetrain', 'vehicleID'])

    def __exclude_hours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.trips = self.trips.dropna(subset=["tripStartClock", "tripEndClock"])

    def __add_string_columns(self, weekday=True, purpose=True, vehicleSegment=True):
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
        if vehicleSegment:
            self.trips = self.trips.replace("groß", "gross")
            self._add_string_column_from_variable(colName="vehicleSegmentStr", varName="vehicleSegment")

    def _drop_redundant_cols(self):
        # Clean-up of temporary redundant columns
        self.trips.drop(
            columns=[
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
        Wrapper function for harmonising and filtering the dataset.
        """
        self._select_columns()
        self.__harmonize_variables()
        self._harmonize_variables_unique_id_names()
        self.__pad_missing_car_segments()
        self.__exclude_hours()
        self._convert_types()
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self._update_end_timestamp()
        self._check_filter_dict()
        self._filter(self.filters)
        self._filter_consistent_hours()
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._subset_vehicle_segment()
        self._cleanup_dataset()
        self.write_output()
        print("Parsing VF dataset completed.")
        return self.activities

