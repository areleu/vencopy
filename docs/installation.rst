.. VencoPy installation documentation file, created on February 11, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _installation:

Installation and Setup
===================================


Requirements and boundary conditions
-------------------------------------

VencoPy runs on Unix and Windows-based operating systems. It requires an installed version of python and the package, 
dependency and environment management tool conda as well as access to the internet for setting up the environment 
(downloading the required packages). VencoPy is consistent with the software application class 1 of the DLR software 
categorization. Versioning is based on semantic versioning i.e. major, minor and fix (X.Y.Z) changes versioning system 
via git-labels. For using VencoPy, git is not required, if you want to contribute to the documentation or the codebase
please see :ref:`How to contribute`

This part of the documentation holds a step-by-step installation guide for VencoPy. 

1.  Create a new environment using a conda console such as the Anaconda Powershell Prompt typing
        
        conda create -n YOUR_ENVIRONMENT
    
    and activate your environment by
        
        conda activate YOUR_ENVIRONMENT

2.  Potentially add: Create some packages from conda before installing from pip

3.  Install VencoPy from the Python Package Index (PyPI) typing

        pip install vencopy

    You now have vencopy installed as a package in your environment (possibly check with *conda list*)

4.  In your conda console navigate to a directory where your vencopy user folder should be installed to, e.g. 
    C:/. Then type 
    
        vencopy
        
    You will be prompted to specify a foldername, type that (e.g. USERFOLDER) and hit enter. The installation will now 
    copy tutorials and default configs to your USERFOLDER together with a default run file, an input and an output 
    folder.
    
5.  Open the USERFOLDER in your preferred IDE (PyCharm, Spider etc.) and select your environment as interpreter.

6.  You have to specify the link to your MiD file. In order to do so, open your localPathConfig.yaml from 
    USERFOLDER/config/ and paste the path to your MiD B2 STATA folder behind MiD17:[HERE]
    
7.  You're now ready to run VencoPy for the first time by typing::
        
        python run.py
        
    or executing from your IDE.

8.  Have fun calculating electric vehicle flexibility!