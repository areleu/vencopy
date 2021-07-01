.. VencoPy installation documentation file, created on February 11, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _installation:

Installation and Setup
===================================


Requirements and boundary conditions
-------------------------------------

VencoPy runs on Unix and Windows-based operating systems. It requires an installed version of python and the package, dependency and environment management tool conda as well as access to the internet for setting up the environment (downloading the required packages). VencoPy is consistent with the software application class 1 of the DLR software categorization. Versioning is based on 
major, minor and fix (X.Y.Z) changes versioning system via git-labels.

This part of the documentation holds a step-by-step installation guide for VencoPy. 

1.  Navigate to a folder to which you want to clone VencoPy. Clone the repository to your local machine using ::
        
        git clone https://gitlab.com/dlr-ve/vencopy.git

2.  Set-up your environment. For this, open a conda console, navigate to the folder of your VencoPy repo and
    enter the following command::
        
        conda env create -f requirements.yml
        conda activate VencoPy_env
    
3.  Configure your config files if you want to use absolute links. This is only needed if you want to reference your own
    local data or want to post-process VencoPy results and write them to a model input folder somewhere on your drive.
    You will find your config file in your repo under /config/config.yaml Input filenames are set to the example files
    shipped with the repo. You may specify labels for file naming in the config under the key "labels".

4.  You're now ready to run VencoPy for the first time by typing::
        
        python run.py

5.  Have fun calculating electric vehicle flexibility!