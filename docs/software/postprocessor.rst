.. venco.py documentation source file, created for sphinx

.. _postprocessor:


PostProcessor Class
===================================


.. image:: ../figures/IOpostprocessor.png
	:width: 800
	:align: center


PostProcessor Input
---------------------------------------------------
**Config File (user_config.yaml):**

* start_weekday: 1 - Number corresponding to start day of the week for annual profile (1=Monday)


**venco.py Classes:**

 * ProfileAggregator class output (5 profiles)


PostProcessor Output
---------------------------------------------------
**Output Functions:**

 * vpPost = PostProcessor(configs=configs, profiles=vpProfile)
 * vpPost.create_annual_profiles()
 * vpPost.normalise()


**Disk Files:**

 * Electric battery drain (.csv)
 * Available charging power (.csv)
 * Uncontrolled charging profile (.csv)
 * Maximum SoC (.csv)
 * Minimum SoC (.csv)