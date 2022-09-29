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
The tsblender is a Python script to manipulate time-series on the command line
or by function calls within Python.  Uses pandas (http://pandas.pydata.org/)
or numpy (http://numpy.scipy.org) for any heavy lifting.

Requirements
------------
* pandas - on Windows this is part scientific Python distributions like
  Python(x,y), Anaconda, or Enthought.

* mando - command line parser

Installation
------------
Should be as easy as running ``pip install tsblender`` or ``easy_install
tsblender`` at any command line.  Not sure on Windows whether this will bring
in pandas, but as mentioned above, if you start with scientific Python
distribution then you shouldn't have a problem.

Usage - Command Line
--------------------
Just run 'tsblender --help' to get a list of subcommands::


    usage: tsblender [-h]
                     {accumulate, add_trend, aggregate, calculate_fdc,
                     calculate_kde, clip, convert, convert_index,
                     convert_index_to_julian, converttz, lag, correlation,
                     createts, date_offset, date_slice, describe, dtw,
                     equation, ewm_window, expanding_window, fill, filter, gof,
                     normalization, pca, pct_change, peak_detection, pick,
                     plot, rank, read, remove_trend, replace, rolling_window,
                     stack, stdtozrxp, tstopickle, unstack, about} ...

    positional arguments:
      {accumulate, add_trend, aggregate, calculate_fdc, calculate_kde, clip,
      convert, convert_index, convert_index_to_julian, converttz, lag,
      correlation, createts, date_offset, date_slice, describe, dtw, equation,
      ewm_window, expanding_window, fill, filter, gof, normalization, pca,
      pct_change, peak_detection, pick, plot, rank, read, remove_trend,
      replace, rolling_window, stack, stdtozrxp, tstopickle, unstack, about}

    accumulate
        Calculate accumulating statistics.
    add_trend
        Add a trend.
    aggregate
        Take a time series and aggregate to specified frequency.
    calculate_fdc
        Return the frequency distribution curve.
    calculate_kde
        Return the kernel density estimation (KDE) curve.
    clip
        Return a time-series with values limited to [a_min, a_max].
    convert
        Convert values of a time series by applying a factor and offset.
    convert_index
        Convert datetime to/from Julian dates from different epochs.
    convert_index_to_julian
        DEPRECATED: Use convert_index instead.
    converttz
        Convert the time zone of the index.
    lag
        Create a series of lagged time-series.
    correlation
        Develop a correlation between time-series and potentially lags.
    createts
        Create empty time series, optionally fill with a value.
    date_offset
        Apply an offset to a time-series.
    date_slice
        Print out data to the screen between start_date and end_date.
    describe
        Print out statistics for the time-series.
    dtw
        Dynamic Time Warping.
    equation
        Apply <equation_str> to the time series data.
    ewm_window
        Calculate exponential weighted functions.
    expanding_window
        Calculate an expanding window statistic.
    fill
        Fill missing values (NaN) with different methods.
    filter
        Apply different filters to the time-series.
    gof
        Will calculate goodness of fit statistics between two time-series.
    normalization
        Return the normalization of the time series.
    pca
        Return the principal components analysis of the time series.
    pct_change
        Return the percent change between times.
    peak_detection
        Peak and valley detection.
    pick
        Will pick a column or list of columns from input.
    plot
        Plot data.
    rank
        Compute numerical data ranks (1 through n) along axis.
    read
        Collect time series from a list of pickle or csv files.
    remove_trend
        Remove a 'trend'.
    replace
        Return a time-series replacing values with others.
    rolling_window
        Calculate a rolling window statistic.
    stack
        Return the stack of the input table.
    stdtozrxp
        Print out data to the screen in a WISKI ZRXP format.
    tstopickle
        Pickle the data into a Python pickled file.
    unstack
        Return the unstack of the input table.
    about
        Display version number and system information.

    optional arguments:
        -h, --help            show this help message and exit

The default for all of the subcommands is to accept data from stdin (typically
a pipe).  If a subcommand accepts an input file for an argument, you can use
"--input_ts=input_file_name.csv", or to explicitly specify from stdin (the
default) "--input_ts='-'".

For the subcommands that output data it is printed to the screen and you can
then redirect to a file.

Usage - API
-----------
You can use all of the command line subcommands as functions.  The function
signature is identical to the command line subcommands.  The return is always
a PANDAS DataFrame.  Input can be a CSV or TAB separated file, or a PANDAS
DataFrame and is supplied to the function via the 'input_ts' keyword.

Simply import tsblender::

    from tsblender import tsblender

    # Then you could call the functions
    ntsd = tsblender.fill(method='linear', input_ts='tests/test_fill_01.csv')

    # Once you have a PANDAS DataFrame you can use that as input to other
    # tsblender functions.
    ntsd = tsblender.aggregate(statistic='mean', agg_interval='daily', input_ts=ntsd)
