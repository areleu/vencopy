.. venco.py documentation source file, created for sphinx

.. _diarybuilder:


DiaryBuilders Level
===================================

.. image:: ../figures/IOdiarybuilder.svg
	:width: 800
	:align: center

DiaryBuilders Input
---------------------------------------------------
**Config File (user_config.yaml):**

* time_resolution: <value> - User-specific time resolution in minutes
* is_week_diary: bool - Determine if the activity data set comprises weekly activity chains (synthesized by WeekDiaryBuilder)


**venco.py Classes:**

 * FlexEstimator class output


DiaryBuilders Output
---------------------------------------------------
**Output Functions:**

 * diary = DiaryBuilder(configs=configs, activities=flex.activities)
 * diary.create_diaries()


**Disk Files:**

 * Electric battery drain (.csv) `drain`
 * Available charging power (.csv) `charging_power`
 * Uncontrolled charging profile (.csv) `uncontrolled_charging`
 * Maximum battery energy level (.csv) `max_battery_level`
 * Minimum battery energy level (.csv) `min_battery_level`


DiaryBuilder Class
#################################################################


TimeDiscretiser Class
#################################################################
