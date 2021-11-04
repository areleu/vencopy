.. VencoPy documentation source file, created for sphinx

.. _tripDiaryBuilder:


TripDiaryBuilder Class
===================================

.. image:: ../figures/IOtripDiaryBuilder.png
   :width: 800


TripDiaryBuilder Input
---------------------------------------------------
**Config File (tripConfig.yaml):** currently empty.

**VencoPy Classes:**

 * DataParser class output

TripDiaryBuilder Output
---------------------------------------------------
**Output Functions:**
 
 * vpDiary = TripDiaryBuilder(configDict=configDict, ParseData=vpData, datasetID=datasetID, debug=True)


**Disk Files:**

 * Hourly boolean dataset with parking and driving time fo all vehicles (.csv)
 * Hourly purpose dataset for all vehicles (.csv)
