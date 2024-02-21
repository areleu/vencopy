..  venco.py introdcution file created on October 20, 2021
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _core:

Core venco.py Levels
===================================

Below is a brief explanation of the six main venco.py classes. For a more detailed explanation about the internal 
workings and the specific outputs of each function, you can click on the hyperlink on the function name.

Interface to the dataset: :ref:`dataparsers`
---------------------------------------------------

The first step in the venco.py framework for being able to estimate EV energy consumption implies accessing a travel 
survey data set, such as the MiD. This is carried out through a parsing interface to the original database. In the 
parsing interface to the data set, three main operations are carried out: the read-in of the travel survey trip data,
stored in .dta or .csv files, filtering and cleaning of the original raw data set and a set of variable replacement 
operations to allow the composition of travel diaries in a second step. In order to have consistent entry data for all
variables and for different data sets, all variable names are harmonised, which includes generating unified data types
and consistent variable naming. The naming convention for the input data variable names and their respective input type
can be specified in the dev-config file. Of the 22 variables, four variables are used for indexing, 11 variables 
characterize the trip time within the year, two variables are used for filtering and five variables characterize the
trip itself. The representation of time may vary between travel surveys. Most travel surveys include motorised, 
non-motorised as well as multi-modal trips. We only select trips that were carried out with a motorized individual 
vehicle as a driver by using a filter defined in the dev_config. Similarly, trips with missing (e.g. missing trip_id,
missing start or end time etc.) or invalid information (e.g. implausible trip distance or inferred speed) are filtered
out. Filters can be easily adapted to other travel survey numeric codes via the config-file. By applying a set of 
filters, the initial database is subset to only contain valid entries representing motorised trips. The last operation
in the parsing of raw travel survey data sets is a harmonization step.
After this steps of creating clean trip chains per vehicle, they are enrhiched by park activities that fill the times in
between the trips and whose locations (e.g. HOME or SHOPPING) are based on the trip purposes of previous trips. At the
end of the process, a clean chain of activities is provided switching between park and trip activities.


Charging infrastructure allocation: :ref:`gridmodellers`
---------------------------------------------------
The charging infrastructure allocation makes use of a basic charging infrastructure model, which assumes the
availability of charging stations when vehicles are parked. Since the analytical focus of the framework lies on a 
regional level (NUTS1-NUTS0), the infrastructure model is kept simple in the current version. Charging availability is
allocated based on a binary TRUE/FALSE mapping to a respective trip purpose in the venco.py user-config. Thus, different
scenarios describing different charging availabilities, e.g. at home or at home and at work etc. can be distinguished,
but neither a regional differentiation nor a charging availability probability or distribution are assumed. At the end
of the application of the GridModeller, a given parking purpose diary parkingType(v, t) is transferred into a binary
grid connection diary connectgrid (v, t) with the same format but consisting only of TRUE/FALSE values.


Flexibility estimation: :ref:`flexestimators`
---------------------------------------------------
There are three integral inputs to the estimation: 1. a profile describing hourly distances for each vehicle 2. a boolean set of profiles describing
if a vehicle is connected to the grid at a given hour 3. techno-economic input assumptions After some filtering and iteration steps, this yields the
minimum and maximum battery constraints. After these steps, six profiles are provided to the user: a battery drain profile (the electricity that flows
out of the vehicle battery each hour for driving), a charging capacity profile (the maximum electricity available for charging in each hour), a
minimum and a maximum SoC (upper and lower limits for the battery SoC), an uncontrolled charging profile (the electricity flow from grid to vehicle
when no control is exerted) and a fuel consumption profile.

The first four profiles can be used as constraints for other models to determine optimal charging strategies, the fifth profile simulates a case,
where charging is not controlled an EVs charge as soon as a charging possibility is available. Lastly, the sixth profile quantifies the demand for
additional fuel for trips that cannot be only by electricity.


Daily travel diary composition: :ref:`diarybuilders`
---------------------------------------------------
In the diaryBuilder, individual trips at the survey day are consolidated into person-specific travel diaries comprising multiple trips (carried out by
car). The daily travel diary composition consists of three main steps: Reformatting the database, allocating trip purposes and merging the obtained
dataframe with other relevant variables from the original database. In the first step, reformatting, the time dimension is transferred from the raw
data (usually in minutes) to the necessary output format (e.g. hours). Each trip is split into shares, which are then assigned to the respective hour
in which they took place, generating an hourly dataframe with a timestamp instead of a dataframe containing single trip entries. Similarly, mileages
driven and the trip purpose are allocated to their respective hour and merged into daily travel diaries. Trips are assumed to determine the respective
person’s stay in the consecutive hours up to the next trip and therefore are related to the charging availability between two trips. Trip purposes
included in surveys may comprise trips carried out for work or education reasons, trips returning to home, trips to shopping facilities and other
leisure activities. Currently, trips whose purpose is not specified are allocated to trips returning to the own household. At the end of the second
venco.py component TripDiaryBuilder, two intermediary data sets are available either directly from the class within Python or from the hard-drive as
.csv files. The first one comprises mileage travel diaries d(v, t) and the second one comprises parking place types derived from trip purposes
parkingType(v, t).


Aggregation to fleet level: :ref:`profileaggregators`
---------------------------------------------------
In the ProfileAggregator, single vehicle profiles are aggregated to fleet level. Depending on the profile, different aggregation approaches are used.


Output postprocessing: :ref:`postprocessors`
---------------------------------------------------
In the PostProcessor, the aggregated weekly timeseries for the fleet are translated into annual timeseries.
An option to normalise the profiles is also provided.
