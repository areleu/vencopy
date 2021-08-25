.. VencoPy getting started documentation file, created on February 11, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _start:

Getting Started and Tutorials
===================================

Tutorials overview
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


To get started with VencoPy a set of tutorials are provided for the user to learn about the different classes in VencoPy and how each of them can be customised.
The tutorials are iPythonNotebooks to be opened with Jupyter Lab.


Tutorial 1: The DataParser class

Tutorial 2: The TripDiaryBuilder class

Tutorial 3: The GridModeler class

Tutorial 4: The FlexEstimator class


Tutorials setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Setting up the Python environment for JupyterLab

Creating new Anaconda environments can be done via the "Anaconda Powershell Prompt" which should be installed on your computer by default. With the following lines we create a new environment called "lab" with the Python version 3.9

Activate the Jupyter lab environment

conda activate lab

Add Firefox to the path variables

In order to call Firefox via the commandline and use it as default browser for JupyterLab we need to add it to the path variables. This can be done by opening the Windows Menu and searching for the tool "Umgebungsvariablen für dieses Konto bearbeiten" (or "Edit the system environment variables"). In the top section of environment variables four your user account you can modify the "Path" variable by double-clicking it, adding a new line with C:\Program Files\Mozilla Firefox and hitting OK to save the changes. If you start a new terminal you should be able to use the command firefox to start the browser.

Start JupyterLab with working directory

Starting your newly created JupyterLab can be done in two ways. First you can open up a terminal, activate the lab environment and then start JupyterLab with arguments for the notebook directory and browse
