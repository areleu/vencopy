.. venco.py documentation master file, created by
   sphinx-quickstart on Tue Feb  4 09:27:27 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



Welcome to venco.py's documentation!
===================================

.. image:: https://img.shields.io/pypi/v/vencopy
   :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/vencopy
   :alt: PyPI - Python Version

.. image:: https://readthedocs.org/projects/vencopy/badge/?version=latest
   :target: https://vencopy.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/pypi/l/vencopy
   :alt: PyPI - License

.. image:: https://badges.gitter.im/vencopy/community.svg
    :target: https://gitter.im/vencopy/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge
    :alt: Chat on Gitter

A data processing tool offering hourly demand and flexibility profiles for future electric vehicle fleets in an aggregated manner.
venco.py is developed at the `Department of Energy Systems Analysis <https://www.dlr.de/ve/en/desktopdefault.aspx/tabid-15971/25909_read-66550/>`_ at the `German Aerospace Center (DLR) <https://www.dlr.de/EN/Home/home_node.html>`_.
In a nutshell, with venco.py you can:
- :ref:`simulate different charging strategies (uncontrolled, controlled charging , V2G) for EV fleets <flexestimator>`
- :ref:`model different grid infrastructure options (home charging, work charging, ..) <gridmodeler>`
- :ref:`analyse charging behaviour based on socio-economic data and trip purpose <dataparser>`



Useful Information about venco.py
------------------
- :ref:`Find a general description of venco.py and its capabilities here <gettingstarted/intro>`
- :ref:`Find the different installation instructions here <gettingstarted/installation>`
- :ref:`Find some frequently asked questions here <faq>`
- :ref:`Find information on planned future features <contribution/releasetimeline>`
- :ref:`Find information on how to contribute <contribution/howtocontribute>`



About
------------------
- Authors: Niklas Wulff, Fabia Miorelli
- Contact: niklas.wulff@dlr.de


Links
-------------------
- Source code: https://gitlab.com/dlr-ve/vencopy
- PyPI release: https://pypi.org/project/vencopy
- License: https://opensource.org/licenses/BSD-3-Clause



.. toctree::
   :caption: Getting Started
   :hidden:
   :maxdepth: 1

   gettingstarted/intro
   gettingstarted/installation
   gettingstarted/start

.. toctree::
   :caption: Software
   :hidden:
   :maxdepth: 1

   software/architecture
   software/classes
   software/functions
   software/inputoutput
   software/codestyle

.. toctree::
   :caption: Research
   :hidden:
   :maxdepth: 1

   research/projects
   research/publications

.. toctree::
   :caption: Contribution
   :hidden:
   :maxdepth: 1

   contribution/releasetimeline
   contribution/howtocontribute
