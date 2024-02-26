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
The charging infrastructure allocation makes use of a basic charging infrastructure model, which infers the
availability of charging stations from parking purposes (e.g. HOME or SHOPPING). These again are inferred from the
purposes of previously carried out trips.
There are two mapping approaches of parking categories to charging station rated power that can be selected in the
gridmodellers section of the user-config, in the option grid_model: Simple and probability-based. 
In the simple model, charging availability is allocated based on a binary TRUE/FALSE mapping to a respective parking
activity purpose in the venco.py user-config. Thus, scenarios describing different charging availabilities, e.g.
at home or at home and at work etc. can be distinguished. Charging is then assumed to be available with a single given
rated power (e.g. 11 kW) for all park activities.   
The second model "probability", refines the simple model in two regards: Firstly, multiple rated powers can be given 
per parking purpose, e.g. HOME charging can be available through single-phase, 16 A (3.7 kW), triple-phase, 16 A (11 kW)
or triple-phase, 32 A (22 kW) chargers. Secondly, top-down probabilities can be given to each rated power for each 
parking activity, e.g. there is a 20% probability of HOME chargers to be triple-phase, 16 A chargers. Here, the
probability of no charging at HOME has to be taken into account. We have to be transparent that despite the
methodological refinement of this second approach, data justifying the specific values of this approach is scarce and
values are mostly future scenarios (relevant possibilities).
At the end of the application of the GridModeller, an additional column "rated_power" is added to the activities data
and filled for each parking activity. 


Flexibility estimation: :ref:`flexestimators`
---------------------------------------------------
The flexibility estimation starts with the activity trip chains of single vehicles and the rated powers. Before the core
estimation, the drain per trip as well as the maximum possible charged energy volume per parking activity are
calculated. 
Then, two cases are differentiated: maximum battery level and minimum battery level, that will make up the mobility-
constrained technical flexibility of the individual vehicle batteries to fulfill their individual electricity demands
for driving. The maximum battery level case follows the logic that charging always takes place as soon as possible and
to the largest extent (highest SOC) possible. The variable for uncontrolled charging is then calculated based on this 
logic. The minimum battery level case on the opposite assumes that energy is charged as late as possible and only to
fulfill the driving demands. 
In order to incorporate the needs, the two logics are applied chronologically from the first activity (may be either
activity_id 0 or 1) to the last activity (for maximum battery level) and anti-chronologically from the last activity to
the first activity. The second approach is needed, because later trips need to be taken into account in first parking
activities in case no charging is available throughout the day. 
Both approaches are implemented as loops over the activity ids (maximum 20 loops per logic per iteration). The variable
calculations for each activity_id is then implemented in a vectorized manner for performance concerns. 
Boundary conditions for state-of-charge (that the SOC at beginning and end of each activity chain are equal) are 
enforced by a number of iterations within which the previous steps are repeated with the start SOC set to the end SOC of
the previous iteration. An auxiliary fuel demand is calculated to identify activity chains that cannot be fulfilled 
purely with battery-based electricity (for the given battery capacity from the user_config).  

Six variables are provided in the activity dataset for each activity (not yet time-discretized profiles) after running 
FlexEstimator.estimate_technical_flexibility_through_iteration(): 
#. drain: Demand for driving of each trip in kWh.
#. rated_power / available_power: Available or rated power for charging for each parking activity in kW.
#. max_battery_level_start / max_battery_level_end: Absolute battery levels for maximum battery level logic at start and
end of each activity (trip AND park activities) as battery state in kWh
#. min_battery_level_start / min_battery_level_end: Absolute battery levels for minimum battery level logic at start and
end of each activity (trip AND park activities) as battery state in kWh
#. uncontrolled_charging: Demand per park activity for charging, if uncontrolled charging is assumed as flow into
battery in kWh. 
#. max_auxiliary_fuel_need / min_auxiliary_fuel_need: Fuel needed additionally to battery energy per trip in l.

The first four profiles can be used as constraints for other models to determine optimal charging strategies, the fifth
profile simulates a case, where charging is not controlled an electric vehicles charge as soon as a charging possibility
is available. Lastly, the sixth profile quantifies the demand for additional fuel for trips that cannot be supplied only
by electricity.


Daily travel diary composition: :ref:`diarybuilders`
---------------------------------------------------

In the DiaryBuilder, activity-specific variables are consolidated into vehicle-specific, time-discrete profiles 
describing e.g. the drain in each 15-minute interval of the day. The temporal resolution is set in the user_config in 
the DiaryBuilder option "time_resolution" in minutes.
The DiaryBuilder is a wrapper class that has an instance of the subclass TimeDiscretizer with again a main function 
TimeDiscretizer.discretize(). In the wrapper class, this function is now applied to each variable (column) of the 
activities dataset that is needed as an output from venco.py. Since in the current application constant, these variables
are always the same (see section :ref:`flexestimators`), they are currently hard-coded in the main function of the 
DiaryBuilder, DiaryBuilder.create_diaries().
There are three different approaches to discretization depending on the variable subject to discretization: distribute,
select and dynamic. The method distribute is applied for energy volumes (in kWh) that shall be allocated to the
different time intervals that represent the whole activity. "select" is used for power values (in kW) such as rated
power. Dynamic is used for battery levels variables and uncontrolled charging, as these have more specific or more 
complex allocation procedures.     
The resulting profiles are one per given variable and contain around 100,000 time-discretized profiles (rows) in the 
temporal resolution specified in the config. Every pofile has the same amount of rows. 


Aggregation to fleet level: :ref:`profileaggregators`
---------------------------------------------------
In the ProfileAggregator, single vehicle profiles are aggregated across all vehicles to gain fleet level profiles. 
Depending on the profile type, different aggregation approaches are used. 
The design pattern is similar as in the diarybuilders. There is one wrapper class, ProfileAggregator, that has an 
instance of the class Aggregator as attribute. This attribute's method perform_aggregation() is then called for each 
profile relevant for the analysis, specifying the profile type as a parameter ('flow' or 'state'). The profiles for 
drain, availability and uncontrolled charging are all flow profiles, whereas the battery level profiles are state 
profiles and thus, are aggregated differently. 
Two options can be given in the user_config for refining profile aggregation: The timespan and the aggregation weights. 
The timespan can be given in the option 'aggregation_timespan', specifying if the resulting profile is a daily profile 
or a weekly profile.
The flow profiles are aggregated using means or weighted means which can be specified in the profileaggregators section
of the user_config under the option 'weight_flow_profiles'. The aggregation does not change the temporal resolution, it
is only related to aggregate the charging profiles from around 100,000 single-vehicle profiles to fleet profiles. Thus,
after aggregation, there are 5 profiles with the temporal timespan (daily or weekly) and the temporal resolution
selected in the diary builder before (e.g. 24 values for daily profiles with hourly resolution). 


Output postprocessing: :ref:`postprocessors`
---------------------------------------------------
In the PostProcessor, two steps happen. First, the aggregated weekly timeseries for the fleet are translated into annual
timeseries by cloning e.g. a week by around 52 times to span a full year. The second purpose is to normalize the 
aggregated fleet profiles in order to provide profiles independent of the specific input and to be able to scale the 
venco.py output in a consecutive stept with feet scenarios or annual energy demands. For the flow profiles, two
normalization bases are applied: The drain and uncontrolled charging profiles are normalized to the annual energy volume
of the profiles to be scaled with an annual energy demand. The charging power profile is normalized by the number of 
vehicles to retrieve a rated charging power per representative vehicle. The two state profiles - minimum and maximum
battery level profiles - are normalized using the battery capacity given in the flexestimaros part of the user_config. 
