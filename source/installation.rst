.. VencoPy installation documentation file, created on February 11, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _installation:

Installation and Setup
===================================


Requirements and boundary conditions
VencoPy runs on Unix and Windows-based operating systems. It requires an installed version of python and the package, dependency and environment management tool conda as well as access to the internet for setting up the environment (downloading the required packages). VencoPy is consistent with the software application class 1 of the DLR software categorization. Versioning is based on 
major, minor and fix (X.Y.Z) changes versioning system via git-labels.

This part of the documentation holds a step-by-step installation guide for VencoPy. 

1.  Set-up your environment. For this, open a console, navigate to the folder of your VencoPy repo and
    enter the following command::

        conda create --file <requirements.yaml>
        conda activate <VencoPy_preRelease>


2.  Configure your config files if you want to use absolute links. This is only needed if you want to reference your own
    local data or want to post-process VencoPy results and write them to a model input folder somewhere on your drive.
    You will find your config file in your repo under /config/config.yaml Input filenames have to be set. 

3.  You're now ready to run VencoPy for the first time by typing::
        
        python venco_main.py

4.  Have fun calculating electric vehicle flexibility!