.. VencoPy documentation source file, created for sphinx

.. _gridModeler:


GridModeler Class
===================================

.. image:: ../figures/IOgridModeler.png
   :width: 800

GridModeler Input
---------------------------------------------------
**Config File (gridConfig.yaml):**

* chargingInfrastructureMappings (assigns True-False to the respective type of charging infrastucture)



GridModeler Output
---------------------------------------------------
**Output Functions:**

* vpGrid = GridModeler(configDict=configDict, datasetID=datasetID)
* vpGrid.assignSimpleGridViaPurposes()
* vpGrid.writeOutGridAvailability()

**Disk File:**

* Hourly boolean dataset with plugging time fo all vehicles (.csv)


