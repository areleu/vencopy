.. venco.py documentation source file, created for sphinx

.. _gridModeling:


Grid Modeling
===================================


This file documents the gridmodelers class of venco.py. It presents a methodology of allocation of charging infrastructure for every hour of a trip  and also calculation of transaction start hour.
There are currently two ways for grid assignment in venco.py.


1.	Simple Grid Assignment
------------------------------------------
	Charging availability is allocated based on a binary TRUE/FALSE mapping to a respective trip purpose and this is specified in venco.py-config.
	It is assumed that the vehicels are charged when they are parked. With this modeling technique, the purpose diary is converted into a binary
	grid connection diary with same format but consisting only of TRUE/FALSE values.


2.	Probability Based Grid Assignment
--------------------------------------------------
	In this technique, a probability based grid modeling is considered. Each purpose is given a certain probability for allocation of charging
	infrastructure. Probability distribution is defined in the venco.py-config as follows,

+-----------------+-----------------+-----------------+
|     Purpose     | Probability     | Charging Power  |
+=================+=================+=================+
| Driving   	  | 1               | 0               |
+-----------------+-----------------+-----------------+
| Home      	  | | 0.5           | | 3.7           |
|                 | | 0.25          | | 11            |
|                 | | 0.15          | | 22            |
|                 | | 0.1           | | 0             |
+-----------------+-----------------+-----------------+
| Work      	  | | 0.5           | | 11            |
|                 | | 0.35          | | 22            |
|                 | | 0.15          | | 0             |
+-----------------+-----------------+-----------------+
| School,      	  | | 0.5           | | 11            |
| Shopping &      | | 0.35          | | 22            |
| Leisure         | | 0.15          | | 0             |
+-----------------+-----------------+-----------------+
| Other      	  | | 0.2           | | 11            |
|                 | | 0.1           | | 22            |
|                 | | 0.7           | | 0             |
+-----------------+-----------------+-----------------+

	Every purpose is assigned a probability between 0 to 1 and based on the probability distribution defined in config,
	a charging station is allotted to that particular purpose. Also, the probability only changes if there is a change of purpose in the next hour.
	We assume that for home charging, the vehicle is connected to the same charging column capacity of 1st hour whenever it is returned home during the whole day.


Transaction Start Hour
------------------------------------
A boolean dataframe is created from plug profiles to identify the transaction start hour. This profile is further helpful to model plug choices in flexEstimator.

Plots
-----------------------------
A total of two plots are plotted in in this class. First plot is of distribution of charging column in plug profiles and second consists of distribution of purposes in the purpose diary.