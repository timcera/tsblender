.. image:: https://github.com/timcera/tsblender/actions/workflows/python-package.yml/badge.svg
    :alt: Tests
    :target: https://github.com/timcera/tsblender/actions/workflows/python-package.yml
    :height: 20

.. image:: https://img.shields.io/coveralls/github/timcera/tsblender
    :alt: Test Coverage
    :target: https://coveralls.io/r/timcera/tsblender?branch=master
    :height: 20

.. image:: https://img.shields.io/pypi/v/tsblender.svg
    :alt: Latest release
    :target: https://pypi.python.org/pypi/tsblender/
    :height: 20

.. image:: https://img.shields.io/pypi/l/tsblender.svg
    :alt: BSD-3 clause license
    :target: https://pypi.python.org/pypi/tsblender/
    :height: 20

.. image:: https://img.shields.io/pypi/dd/tsblender.svg
    :alt: tsblender downloads
    :target: https://pypi.python.org/pypi/tsblender/
    :height: 20

.. image:: https://img.shields.io/pypi/pyversions/tsblender
    :alt: PyPI - Python Version
    :target: https://pypi.org/project/tsblender/
    :height: 20

tsblender - Quick Guide
=======================
The tsblender is a pure python re-write of TSPROC (Time Series PROCessor) from
the USGS and John Doherty.  When finished it will be a superset of TSPROC
functionality and a more robust and flexible tool for time series analysis.

This is not complete and is still under development.

Requirements
------------
* python 3.8 or higher

Installation
------------
Should be as easy as running ``pip install tsblender`` or ``conda install -c
conda-forge -c timcera tsblender`` at any command line.

Usage - Command Line
--------------------
Just run 'tsblender --help' to get a list of subcommands::


    usage: tsblender [-h]
                     {run, about) ...

    positional arguments:
      {run, about}

    about
        Display version number and system information.
    run
        Run a tsblender script file.

    optional arguments:
        -h, --help            show this help message and exit
