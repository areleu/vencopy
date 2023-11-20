.. venco.py documentation source file, created for sphinx

.. _profileaggregator:


ProfileAggregator Class
===================================


.. image:: ../figures/IOprofileaggregator.svg
	:width: 800
	:align: center

TripDiaryBuilder Input
---------------------------------------------------
**Config File (user_config.yaml):**

* aggregation_timespan: weekly - Options are: daily, weekly
* weight_flow_profiles: bool - Currently only used for flow profile aggregation
* alpha: 10 - Percentile to exclude for state profiles aggregation


**venco.py Classes:**

 * DiaryBuilder class output (5 profiles)


TripDiaryBuilder Output
---------------------------------------------------
**Output Functions:**

 * profile = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
 * profile.aggregate_profiles()


**Disk Files:**

 * Electric battery drain (.csv)
 * Available charging power (.csv)
 * Uncontrolled charging profile (.csv)
 * Maximum battery energy level (.csv)
 * Minimum battery energy level (.csv)


Aggregation Approaches
---------------------------------------------------

The aggregation approach implemented in venco.py varies according to the considered profile.
Below the different approaches are illustrated.


Profile for uncontrolled charging `uncontrolled_charging`
#################################################################


Profile for the electric demand `drain`
#################################################################


Profile for the charging capacity of the fleet `charging_power`
############################################################


Maximum and minimum battery level profile `max_battery_level` and `min_battery_level`
#################################################################