==========
datavisapp
==========

a data visualisation app *for science*


Usage
=====

The module ``create_db.py`` contains functions for feeding data into a SQLite database.


Installation
============

Clone this repo from GitHub; then install the dependencies and this package::

   git clone git@github.com:edithvee/datavisapp.git
   cd datavisapp
   pip install -r requirements.txt
   pip install .


Development
===========

Follow the installation instructions above, but use the ``-e`` flag with pip.
Also install the development tools::

   pip install -e .
   pip install pytest flake8

The tests can then be run with::

   pytest tests

The code style can be checked with::

   flake8 datavisapp tests

