"""Collection of functions for the manipulation of time series."""

import datetime
import os.path
import re
import sys
from collections import OrderedDict
from contextlib import suppress
from typing import Literal, Optional, Union

import cltoolbox
import numpy as np
import pandas as pd
from dateutil.parser import parse
from fortranformat import FortranRecordWriter
from hydrotoolbox import hydrotoolbox
from hydrotoolbox.hydrotoolbox import baseflow_sep
from matplotlib import pyplot as plt
from numpy import abs
from numpy import arccos as acos
from numpy import arcsin as asin
from numpy import arctan as atan
from numpy import cos, cosh, exp, isin, log, log10, sin, sinh, sqrt, tan, tanh
from plottoolbox import plottoolbox
from pydantic import Field
from pytest import param
from typing_extensions import Annotated

try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from scipy import signal
from toolbox_utils import tsutils


def warning(message: str):
    """Print a warning message."""
    print(tsutils.error_wrapper(message), file=sys.stderr)


@cltoolbox.command()
def about():
    """Display version number and system information."""
    tsutils.about(__name__)


deprecated = {
    "GET_MUL_SERIES_PLOTGEN": "Use multiple GET_SERIES_PLOTGEN blocks or a rolled up GET_SERIES_PLOTGEN block instead.",
    "GET_MUL_SERIES_GSFLOW_GAGE": "Use multiple GET_SERIES_GSFLOW_GAGE blocks or a rolled up GET_SERIES_GSFLOW_GAGE block instead.",
    "GET_MUL_SERIES_SSF": "Use multiple GET_SERIES_SSF blocks or a rolled up GET_SERIES_SSF block instead.",
    "GET_MUL_SERIES_STATVAR": "Use multiple GET_SERIES_STATVAR blocks or a rolled up GET_SERIES_STATVAR block instead.",
}


def natural_keys(text):
    """Sort strings with embedded numbers naturally."""
    if not isinstance(text, str):
        return text
    if ":" not in text:
        return text
    words = text.split(":")
    lets = words[0][:2]
    nums = words[0][2:]
    if len(nums) == 1:
        nums = "0" + nums
    return lets + nums


class Tables:
    """Class to hold the tables."""

    def __init__(self):
        self.series = {}
        self.current_series = {}
        self.series_dates = {}

        self.v_table = pd.DataFrame()
        self.v_table_metadata = {}

        self.c_table = pd.DataFrame()
        self.c_table_metadata = pd.DataFrame()

        self.s_table = pd.DataFrame()
        self.s_table_metadata = {}

        self.e_table = pd.DataFrame()
        self.e_table_tot = pd.DataFrame()
        self.e_table_metadata = {}

        self.g_table = pd.DataFrame()
        self.g_table_metadata = {}

        self.date_format = "%Y-%m-%d"
        self.line_number = 0
        self.block_name = ""

        self.ofile = ""

        self.last_list_output_parameters = {}

        self.funcs = {
            "SETTINGS": {
                "args": ["context", "date_format"],
                "kwds": {},
                "f": self.settings,
            },
            "COPY": {
                "args": ["CONTEXT", "NEW_ENTITY_NAME"],
                "kwds": {
                    "SERIES_NAME": None,
                    "C_TABLE_NAME": None,
                    "S_TABLE_NAME": None,
                    "V_TABLE_NAME": None,
                    "E_TABLE_NAME": None,
                    "G_TABLE_NAME": None,
                    "OVERWRITE": False,
                },
                "f": self.copy,
            },
            "DIGITAL_FILTER": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME", "FILTER_TYPE"],
                "kwds": {
                    "FILTER_PASS": None,
                    "CUTOFF_FREQUENCY": None,
                    "CUTOFF_FREQUENCY_1": None,
                    "CUTOFF_FREQUENCY_2": None,
                    "STAGES": 1,
                    "ALPHA": None,
                    "PASSES": 1,
                    "REVERSE_SECOND_STAGE": False,
                    "CLIP_INPUT": False,
                    "CLIP_ZERO": False,
                },
                "f": self.digital_filter,
            },
            "ERASE_ENTITY": {
                "args": ["CONTEXT"],
                "kwds": {
                    "SERIES_NAME": None,
                    "C_TABLE_NAME": None,
                    "S_TABLE_NAME": None,
                    "V_TABLE_NAME": None,
                    "E_TABLE_NAME": None,
                    "G_TABLE_NAME": None,
                },
                "f": self.erase_entity,
            },
            "EXCEEDANCE_TIME": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_E_TABLE_NAME",
                    "EXCEEDANCE_TIME_UNITS",
                ],
                "kwds": {
                    "UNDER_OVER": "over",
                    "FLOW": None,
                    "DELAY": None,
                },
                "f": self.exceedance_time,
            },
            "FLOW_DURATION": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                ],
                "kwds": {
                    "NEW_G_TABLE_NAME": None,
                    "NEW_TABLE_NAME": None,
                    "EXCEEDANCE_PROBABILITIES": [
                        99.5,
                        99,
                        98,
                        95,
                        90,
                        75,
                        50,
                        25,
                        10,
                        5,
                        2,
                        1,
                        0.5,
                    ],
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.flow_duration,
            },
            "GET_MUL_SERIES_GSFLOW_GAGE": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "DATA_TYPE",
                    "NEW_SERIES_NAME",
                    "MODEL_REFERENCE_DATE",
                    "MODEL_REFERENCE_TIME",
                ],
                "kwds": {
                    "TIME_UNITS_PER_DAY": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_gsflow_gage,
            },
            "GET_SERIES_GSFLOW_GAGE": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "DATA_TYPE",
                    "NEW_SERIES_NAME",
                    "MODEL_REFERENCE_DATE",
                    "MODEL_REFERENCE_TIME",
                ],
                "kwds": {
                    "TIME_UNITS_PER_DAY": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_gsflow_gage,
            },
            "GET_SERIES_CSV": {
                "args": ["CONTEXT", "FILE", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                    "USECOL": None,
                },
                "f": self.get_series_csv,
            },
            "GET_SERIES_HSPFBIN": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "NEW_SERIES_NAME",
                    "INTERVAL",
                    "OPERATIONTYPE",
                    "ID",
                    "VARIABLE",
                ],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_hspfbin,
            },
            "GET_MUL_SERIES_PLOTGEN": {
                "args": ["CONTEXT", "FILE", "LABEL", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_plotgen,
            },
            "GET_SERIES_PLOTGEN": {
                "args": ["CONTEXT", "FILE", "LABEL", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_plotgen,
            },
            "GET_MUL_SERIES_SSF": {
                "args": ["CONTEXT", "FILE", "SITE", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_ssf,
            },
            "GET_SERIES_SSF": {
                "args": ["CONTEXT", "FILE", "SITE", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_ssf,
            },
            "GET_MUL_SERIES_STATVAR": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "VARIABLE_NAME",
                    "LOCATION_ID",
                    "NEW_SERIES_NAME",
                ],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_statvar,
            },
            "GET_SERIES_STATVAR": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "VARIABLE_NAME",
                    "LOCATION_ID",
                    "NEW_SERIES_NAME",
                ],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_statvar,
            },
            "GET_SERIES_SWMMBIN": {
                "args": ["CONTEXT"],
                "kwds": {
                    "SERIES_NAME": None,
                },
                "f": lambda x: x,
            },
            "GET_SERIES_TETRAD": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "NEW_SERIES_NAME",
                    "WELL_NAME",
                    "OBJECT_NAME",
                    "MODEL_REFERENCE_DATE",
                    "MODEL_REFERENCE_TIME",
                ],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_tetrad,
            },
            "GET_SERIES_UFORE_HYDRO": {
                "args": [
                    "CONTEXT",
                    "FILE",
                    "NEW_SERIES_NAME",
                    "MODEL_REFERENCE_DATE",
                    "MODEL_REFERENCE_TIME",
                    "TIME_INCREMENT",
                ],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_ufore_hydro,
            },
            "GET_SERIES_WDM": {
                "args": ["CONTEXT", "NEW_SERIES_NAME", "FILE", "DSN"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                    "DEF_TIME": "00:00:00",
                    "FILTER": None,
                },
                "f": self.get_series_wdm,
            },
            "GET_SERIES_XLSX": {
                "args": ["CONTEXT", "NEW_SERIES_NAME", "FILE"],
                "kwds": {
                    "SHEET": 1,
                    "COLUMN": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_xlsx,
            },
            "HYDRO_EVENTS": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_SERIES_NAME",
                    "RISE_LAG",
                    "FALL_LAG",
                ],
                "kwds": {
                    "WINDOW": 1,
                    "MIN_PEAK": 0,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.hydro_events,
            },
            "HYDRO_PEAKS": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME"],
                "kwds": {
                    "WINDOW": 1,
                    "MIN_PEAK": 0,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.hydro_peaks,
            },
            "HYDROLOGIC_INDICES": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_G_TABLE_NAME"],
                "kwds": {
                    "USE_MEDIAN": False,
                    "DRAINAGE_AREA": 1,
                    "STREAM_CLASSIFICATION": None,
                    "FLOW_COMPONENT": None,
                    "MA": None,
                    "ML": None,
                    "MH": None,
                    "FL": None,
                    "FH": None,
                    "DL": None,
                    "DH": None,
                    "TA": None,
                    "TL": None,
                    "TH": None,
                    "RA": None,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                    "CURRENT_DEFINITIONS": False,
                },
                "f": self.hydrologic_indices,
            },
            "LIST_OUTPUT": {
                "args": ["CONTEXT", "FILE"],
                "kwds": {
                    "SERIES_FORMAT": "",
                    "SERIES_NAME": (),
                    "C_TABLE_NAME": (),
                    "S_TABLE_NAME": (),
                    "V_TABLE_NAME": (),
                    "E_TABLE_NAME": (),
                    "G_TABLE_NAME": (),
                },
                "f": self.list_output,
            },
            "MOVE": {
                "args": ["CONTEXT", "NEW_ENTITY_NAME"],
                "kwds": {
                    "SERIES_NAME": None,
                    "C_TABLE_NAME": None,
                    "S_TABLE_NAME": None,
                    "V_TABLE_NAME": None,
                    "E_TABLE_NAME": None,
                    "G_TABLE_NAME": None,
                    "OVERWRITE": False,
                },
                "f": self.move,
            },
            "NEW_SERIES_UNIFORM": {
                "args": [
                    "CONTEXT",
                    "NEW_SERIES_NAME",
                    "NEW_SERIES_VALUE",
                    "TIME_INTERVAL",
                    "TIME_UNIT",
                    "DATE_1",
                    "TIME_1",
                    "DATE_2",
                    "TIME_2",
                ],
                "kwds": {},
                "f": self.new_series_uniform,
            },
            "NEW_TIME_BASE": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME", "TB_SERIES_NAME"],
                "kwds": {},
                "f": self.new_time_base,
            },
            "PERIOD_STATISTICS": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_SERIES_NAME",
                    "STATISTIC",
                    "PERIOD",
                    "TIME_ABSCISSA",
                ],
                "kwds": {
                    "YEAR_TYPE": "water_high",
                    "LOG": "no",
                    "POWER": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.period_statistics,
            },
            "PLOT": {
                "args": ["CONTEXT", "SERIES_NAME", "FILE"],
                "kwds": {
                    "KIND": "line",
                    "XLABEL": "",
                    "YLABEL": "",
                    "TITLE": "",
                    "FIGSIZE_WIDTH": 10,
                    "FIGSIZE_HEIGHT": 6.0,
                    "LEGEND": True,
                    "LEGEND_NAMES": None,
                    "STYLE": None,
                    "LOGX": False,
                    "LOGY": False,
                    "XLIM_MIN": None,
                    "XLIM_MAX": None,
                    "YLIM_MIN": None,
                    "YLIM_MAX": None,
                    "SECONDARY_Y": None,
                    "MARK_RIGHT": True,
                    "GRID": False,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.plot,
            },
            "REDUCE_TIME_SPAN": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.reduce_time_span,
            },
            "SERIES_BASE_LEVEL": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "SUBSTITUTE",
                    "NEW_SERIES_NAME",
                    "BASE_LEVEL_SERIES_NAME",
                    "BASE_LEVEL_DATE",
                    "BASE_LEVEL_TIME",
                ],
                "kwds": {
                    "NEGATE": False,
                },
                "f": self.series_base_level,
            },
            "SERIES_CLEAN": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_SERIES_NAME",
                    "SUBSTITUTE_VALUE",
                ],
                "kwds": {
                    "LOWER_ERASE_BOUNDARY": None,
                    "UPPER_ERASE_BOUNDARY": None,
                },
                "f": self.series_clean,
            },
            "SERIES_COMPARE": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME_SIM",
                    "SERIES_NAME_OBS",
                    "NEW_C_TABLE_NAME",
                ],
                "kwds": {
                    "SERIES_NAME_BASE": None,
                    "BIAS": "no",
                    "STANDARD_ERROR": "no",
                    "RELATIVE_BIAS": "no",
                    "RELATIVE_STANDARD_ERROR": "no",
                    "NASH_SUTCLIFFE": "no",
                    "COEFFICIENT_OF_EFFICIENCY": "no",
                    "INDEX_OF_AGREEMENT": "no",
                    "VOLUMETRIC_EFFICIENCY": "no",
                    "EXPONENT": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.series_compare,
            },
            "SERIES_DIFFERENCE": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME"],
                "kwds": {},
                "f": self.series_difference,
            },
            "SERIES_DISPLACE": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_SERIES_NAME",
                    "LAG_INCREMENT",
                    "FILL_VALUE",
                ],
                "kwds": {},
                "f": lambda x: x,
            },
            "SERIES_EQUATION": {
                "args": ["CONTEXT", "NEW_SERIES_NAME", "EQUATION"],
                "kwds": {},
                "f": self.series_equation,
            },
            "SERIES_STATISTICS": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_S_TABLE_NAME"],
                "kwds": {
                    "SUM": "no",
                    "MEAN": "no",
                    "MEDIAN": "no",
                    "STD_DEV": "no",
                    "MAXIMUM": "no",
                    "MINIMUM": "no",
                    "RANGE": "no",
                    "LOG": "no",
                    "POWER": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.series_statistics,
            },
            "USGS_HYSEP": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_SERIES_NAME",
                    "HYSEP_TYPE",
                    "TIME_INTERVAL",
                ],
                "kwds": {
                    "AREA": 1.0,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.usgs_hysep,
            },
            "V_TABLE_TO_SERIES": {
                "args": ["CONTEXT", "NEW_SERIES_NAME", "V_TABLE_NAME", "TIME_ABSCISSA"],
                "kwds": {},
                "f": self.v_table_to_series,
            },
            "VOLUME_CALCULATION": {
                "args": [
                    "CONTEXT",
                    "SERIES_NAME",
                    "NEW_V_TABLE_NAME",
                    "FLOW_TIME_UNITS",
                ],
                "kwds": {
                    "DATE_FILE": None,
                    "AUTOMATIC_DATES": None,
                    "FACTOR": 1.0,
                },
                "f": self.volume_calculation,
            },
            "WRITE_PEST_FILES": {
                "args": ["CONTEXT", "NEW_PEST_CONTROL_FILE", "NEW_INSTRUCTION_FILE"],
                "kwds": {
                    "SERIES_NAME": None,
                    "TEMPLATE_FILE": None,
                    "MODEL_INPUT_FILE": None,
                    "PARAMETER_DATA_FILE": None,
                    "PARAMETER_GROUP_FILE": None,
                    "OBSERVATION_SERIES_NAME": (),
                    "MODEL_SERIES_NAME": (),
                    "SERIES_WEIGHTS_EQUATION": (),
                    "SERIES_WEIGHTS_MIN_MAX": (),
                    "OBSERVATION_S_TABLE_NAME": (),
                    "MODEL_S_TABLE_NAME": (),
                    "S_TABLE_WEIGHTS_EQUATION": (),
                    "S_TABLE_WEIGHTS_MIN_MAX": (),
                    "OBSERVATION_V_TABLE_NAME": (),
                    "MODEL_V_TABLE_NAME": (),
                    "V_TABLE_WEIGHTS_EQUATION": (),
                    "V_TABLE_WEIGHTS_MIN_MAX": (),
                    "OBSERVATION_E_TABLE_NAME": (),
                    "MODEL_E_TABLE_NAME": (),
                    "E_TABLE_WEIGHTS_EQUATION": (),
                    "E_TABLE_WEIGHTS_MIN_MAX": (),
                    "OBSERVATION_G_TABLE_NAME": (),
                    "MODEL_G_TABLE_NAME": (),
                    "G_TABLE_WEIGHTS_EQUATION": (),
                    "G_TABLE_WEIGHTS_MIN_MAX": (),
                    "AUTOMATIC_USER_INTERVENTION": "no",
                    "TRUNCATED_SVD": 2.0e-7,
                    "MODEL_COMMAND_LINE": None,
                },
                "f": self.write_pest_files,
            },
        }

    from .get_series.get_series_csv import get_series_csv
    from .get_series.get_series_gsflow_gage import get_series_gsflow_gage
    from .get_series.get_series_hspfbin import get_series_hspfbin
    from .get_series.get_series_plotgen import get_series_plotgen
    from .get_series.get_series_ssf import get_series_ssf
    from .get_series.get_series_statvar import get_series_statvar
    from .get_series.get_series_tetrad import get_series_tetrad
    from .get_series.get_series_ufore_hydro import get_series_ufore_hydro
    from .get_series.get_series_wdm import get_series_wdm
    from .get_series.get_series_xlsx import get_series_xlsx
    from .series.series_base_level import series_base_level
    from .series.series_clean import series_clean
    from .series.series_compare import (
        coefficient_of_efficiency,
        index_of_agreement,
        series_compare,
    )
    from .series.series_difference import series_difference
    from .series.series_displace import series_displace
    from .series.series_equation import _series_equation, series_equation
    from .series.series_statistics import series_statistics

    def _get_c_table(self, c_table_name: str):
        """Get a c_table from the c_table dataframe."""
        return self.c_table[c_table_name.upper()].dropna()

    def _get_e_table(self, e_table_name: str):
        """Get a e_table from the e_table dataframe."""
        return self.e_table[e_table_name.upper()].dropna()

    def _get_g_table(self, g_table_name: str):
        """Get a g_table from the g_table dataframe."""
        return self.g_table[g_table_name.upper()].dropna()

    def _get_s_table(self, s_table_name: str):
        """Get a s_table from the s_table dataframe."""
        return self.s_table[s_table_name.upper()].dropna()

    def _get_series(self, series_name: str):
        """Get a series from the series dataframe."""
        series_name = series_name.upper()
        whichdf = self.current_series[series_name]
        return self.series[whichdf][series_name]

    def _get_v_table(self, v_table_name: str):
        """Get a v_table from the v_table dataframe."""
        return self.v_table[v_table_name.upper()].dropna()

    def _join(
        self,
        new_name,
        series=None,
        v_table=None,
        c_table=None,
        s_table=None,
        e_table=None,
        e_table_tot=None,
        g_table=None,
    ):
        new_name = new_name.strip().upper()
        if series is not None:
            if new_name in self.current_series:
                raise ValueError(f"{new_name} is already a named series")
            series = pd.DataFrame(series)
            series.columns = [new_name]
            self.series_dates[new_name] = [series.index[0], series.index[-1]]
            self.current_series[new_name] = series.index.freqstr
            self.series[series.index.freqstr] = self.series.get(
                series.index.freqstr, pd.DataFrame()
            ).join(series, how="outer")
        if v_table is not None:
            if new_name in self.v_table:
                raise ValueError(f"{new_name} is already a named v_table")
            v_table = pd.DataFrame(v_table)
            v_table.columns = [new_name]
            try:
                self.v_table = self.v_table.join(v_table, how="outer")
            except ValueError:
                self.v_table = v_table
        if c_table is not None:
            c_table.columns = [new_name]
            inname = c_table.columns[0]
            if inname in self.c_table.columns:
                raise ValueError(f"{inname} is already a named c_table")
            self.c_table = self.c_table.join(c_table, how="outer")
        if s_table is not None:
            s_table.columns = [new_name]
            inname = s_table.columns[0]
            if inname in self.s_table.columns:
                raise ValueError(f"{inname} is already a named s_table")
            self.s_table = self.s_table.join(s_table, how="outer")
        if e_table is not None:
            inname = e_table.columns[0]
            if inname in self.e_table.columns:
                raise ValueError(f"{inname} is already a named e_table")
            self.e_table = self.e_table.join(e_table, how="outer")
        if e_table_tot is not None:
            inname = e_table_tot.columns[0]
            if inname in self.e_table_tot.columns:
                raise ValueError(f"{inname} is already a named e_table")
            self.e_table_tot = self.e_table_tot.join(e_table_tot, how="outer")
        if g_table is not None:
            if new_name in self.g_table:
                raise ValueError(f"{new_name} is already a named g_table")
            g_table = pd.DataFrame(g_table)
            g_table.columns = [new_name]
            try:
                self.g_table = self.g_table.join(g_table, how="outer")
            except ValueError:
                self.g_table = g_table

    def _normalize_times(self, time="00:00:00"):
        if time is None:
            time = "00:00:00"
        words = time.split(":")
        if len(words) == 3:
            hours, minutes, seconds = (int(i) for i in words)
        elif len(words) == 2:
            hours, minutes = (int(i) for i in words)
            seconds = 0
        elif len(words) == 1:
            hours = int(words[0])
            minutes = 0
            seconds = 0
        return hours, minutes, seconds

    def _normalize_datetimes(self, date, time="00:00:00"):
        if date is None:
            return None
        hours, minutes, seconds = self._normalize_times(time)
        delta = pd.Timedelta(days=0)
        if int(hours) == 24:
            hours = 0
            delta = pd.Timedelta(days=1)
        return (
            parse(f"{date} {hours:02}:{minutes:02}:{seconds:02}") + delta
        ).isoformat()

    def _normalize_bools(self, torf):
        if torf is True or torf is False:
            return torf
        if isinstance(torf, str):
            if torf.lower() in ("y", "yes"):
                return True
            if torf.lower() in ("n", "no"):
                return False
        return bool(torf)

    def _there_can_be_only_one(self, inlist, errstr):
        inlist = [self._normalize_bools(i) for i in inlist]
        inlist = [i for i in inlist if i is True]
        if len(inlist) > 1:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    Only one of the following can be True: {errstr}
                    """
                )
            )

    @validate_call
    def _prepare_series(
        self,
        series_name,
        log: bool = False,
        power: Annotated[float, Field(gt=0)] = 1,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
        **minmaxmeans,
    ):
        if power == 1:
            powertest = False
        self._there_can_be_only_one([log, powertest], "'LOG' or 'POWER'")

        log = self._normalize_bools(log)

        test = [log, any(minmaxmeans.keys())]
        self._there_can_be_only_one(
            test, "'LOG' or any of the following: 'MAXMEAN_n', 'MINMEAN_n'"
        )

        test = [powertest]
        test.append(any(minmaxmeans.keys()))
        self._there_can_be_only_one(
            test, "'POWER' or any of the following: 'MAXMEAN_n', 'MINMEAN_n'"
        )
        series = self._get_series(series_name)
        series = tsutils.common_kwds(
            series,
            start_date=self._normalize_datetimes(date_1, time_1),
            end_date=self._normalize_datetimes(date_2, time_2),
        )
        series = series.asfreq(series.index.freqstr)

        if log:
            series = np.log10(series)

        if powertest:
            series = np.power(series, power)

        series = series.loc[
            series.first_valid_index() : series.last_valid_index(), series.columns[0]
        ]
        return series

    def _read_file(self, data_file):
        with open(data_file, encoding="ascii") as fpi:
            for line_number, line in enumerate(fpi):
                nline = line.strip()

                # Handle comment lines and partial comment lines.  Everything from
                # a "#" to the end of the line is a comment.
                with suppress(ValueError):
                    nline = nline[: nline.index("#")].rstrip()

                # Handle blank lines.
                if not nline:
                    continue

                yield nline, line_number

    @validate_call
    def digital_filter(
        self,
        series_name: str,
        new_series_name: str,
        filter_type: Literal["butterworth", "baseflow_separation"],
        filter_pass: Optional[Literal["low", "high", "band"]] = None,
        cutoff_frequency: Optional[float] = None,
        cutoff_frequency_1: Optional[float] = None,
        cutoff_frequency_2: Optional[float] = None,
        stages: Union[int, Literal[1, 2, 3]] = 1,
        alpha: Optional[float] = None,
        passes: Union[int, Literal[1, 3]] = 1,
        reverse_second_stage: Optional[Union[bool, Literal["yes", "no"]]] = None,
        clip_input: Union[bool, Literal["yes", "no"]] = False,
        clip_zero: Union[bool, Literal["yes", "no"]] = False,
    ):
        """Filter a time series."""
        series = self._get_series(series_name)
        if filter_type.lower() in ("butterworth"):
            scipy_pass = {
                "low": "lowpass",
                "high": "highpass",
                "band": "bandpass",
                "bandpass": "bandpass",
                "bandstop": "bandstop",
            }[filter_pass]
            fs = {"H": 24, "T": 24 * 60, "D": 1, "M": 1 / 30.5, "A": 1 / 365.25}[
                series.index.freqstr
            ]
            if scipy_pass in ("low", "high"):
                cf = cutoff_frequency
            else:
                cf = [cutoff_frequency_1, cutoff_frequency_2]
            if filter_type.lower() == "butterworth":
                sos = signal.butter(stages, cf, scipy_pass, fs=fs, output="sos")
            filtered = signal.sosfilt(sos, series)
        elif filter_type.lower() in ("baseflow_separation"):
            filtered = baseflow_sep.chapman(series)
        self._join(new_series_name, series=filtered)

    @validate_call
    def copy(
        self,
        new_entity_name: str,
        series_name: str = "",
        v_table_name: str = "",
        c_table_name: str = "",
        s_table_name: str = "",
        e_table_name: str = "",
        g_table_name: str = "",
        overwrite: Union[bool, Literal["yes", "no"]] = False,
    ):
        """Copy an entity."""
        overwrite = self._normalize_bools(overwrite)

        cnt = 0

        if series_name:
            cnt += 1
            series = self._get_series(series_name)
            if overwrite:
                self.erase_entity(series_name=new_entity_name.upper())
            self._join(new_entity_name, series=series)

        if v_table_name:
            cnt += 1
            v_table = self._get_v_table(v_table_name)
            if overwrite:
                self.erase_entity(v_table_name=new_entity_name.upper())
            self._join(new_entity_name, v_table=v_table)

        if c_table_name:
            cnt += 1
            c_table = self._get_c_table(c_table_name)
            if overwrite:
                self.erase_entity(c_table_name=new_entity_name.upper())
            self._join(new_entity_name, c_table=c_table)

        if s_table_name:
            cnt += 1
            s_table = self._get_s_table(s_table_name)
            if overwrite:
                self.erase_entity(s_table_name=new_entity_name.upper())
            self._join(new_entity_name, s_table=s_table)

        if e_table_name:
            cnt += 1
            e_table = self._get_e_table(e_table_name)
            if overwrite:
                self.erase_entity(e_table_name=new_entity_name.upper())
            self._join(new_entity_name, e_table=e_table)

        if g_table_name:
            cnt += 1
            g_table = self._get_g_table(g_table_name)
            if overwrite:
                self.erase_entity(g_table_name=new_entity_name.upper())
            self._join(new_entity_name, g_table=g_table)

        if cnt != 1:
            raise ValueError(
                tsutils.error_wrapper(
                    """
                    Exactly one of the following must be specified:
                    'series_name', 'v_table_name', 'c_table_name', 's_table_name',
                    'e_table_name', 'g_table_name'
                    """
                )
            )

    @validate_call
    def erase_entity(
        self,
        series_name: str = "",
        v_table_name: str = "",
        c_table_name: str = "",
        s_table_name: str = "",
        e_table_name: str = "",
        g_table_name: str = "",
    ):
        """Erase a column in a series, or *_table."""
        if series_name:
            series = series_name.upper()
            self.series[self.current_series[series]] = self.series[
                self.current_series[series]
            ].drop(series, axis="columns")
            del self.current_series[series]
            del self.series_dates[series]

        if v_table_name:
            self.v_table = self.v_table.drop(v_table_name.upper(), axis="columns")
            del self.v_table_metadata[v_table_name.upper()]

        if c_table_name:
            self.c_table = self.c_table.drop(c_table_name.upper(), axis="columns")
            del self.c_table_metadata[c_table_name.upper()]

        if s_table_name:
            self.s_table = self.s_table.drop(s_table_name.upper(), axis="columns")
            del self.s_table_metadata[s_table_name.upper()]

        if e_table_name:
            self.e_table = self.e_table.drop(e_table_name.upper(), axis="columns")
            self.e_table_tot = self.e_table_tot.drop(
                e_table_name.upper(), axis="columns"
            )
            del self.e_table_metadata[e_table_name.upper()]

        if g_table_name:
            self.g_table = self.g_table.drop(g_table_name.upper(), axis="columns")
            del self.g_table_metadata[g_table_name.upper()]

    @validate_call
    def exceedance_time(
        self,
        series_name: str,
        new_e_table_name: str,
        exceedance_time_units: Literal[
            "year",
            "month",
            "day",
            "hour",
            "min",
            "sec",
            "years",
            "months",
            "days",
            "hours",
            "mins",
            "secs",
        ],
        under_over: Literal["over", "under"] = "over",
        **flow_delay,
    ):
        """Calculate the exceedance time for a time series."""
        series = self._get_series(series_name)

        year = datetime.timedelta(days=365, hours=6, minutes=9, seconds=9)
        punits = {
            "year": year,
            "month": year / 12,
            "day": datetime.timedelta(days=1),
            "hour": datetime.timedelta(hours=1),
            "min": datetime.timedelta(minutes=1),
            "sec": datetime.timedelta(seconds=1),
            "years": year,
            "months": year / 12,
            "days": datetime.timedelta(days=1),
            "hours": datetime.timedelta(hours=1),
            "mins": datetime.timedelta(minutes=1),
            "secs": datetime.timedelta(seconds=1),
        }[exceedance_time_units.lower().rstrip("s")]

        input_flow = flow_delay.get("flow")
        input_flow = tsutils.make_list(input_flow)
        if isinstance(input_flow, (int, float)):
            input_flow = [input_flow]

        input_delay = flow_delay.get("delay", 0)
        if isinstance(input_delay, (int, float)):
            input_delay = [input_delay]
        if input_delay == [0]:
            input_delay = [0] * len(input_flow)

        e_table = hydrotoolbox.exceedance_time(
            *input_flow,
            input_ts=series,
            time_units=exceedance_time_units,
            under_over=under_over,
            delays=input_delay,
        )

        # LIST_OUTPUT includes a fraction of the total time column and this is
        # an easy way (by using series.min or series.max) to get the total
        # time.
        tot_threshold = series.min() if under_over == "over" else series.max()
        tot_table = hydrotoolbox.exceedance_time(
            tot_threshold,
            input_ts=series,
            time_units=exceedance_time_units,
            under_over=under_over,
            delays=[0],
        )

        input_delay = [i * punits for i in input_delay]
        keys = []
        etv = []
        e_totv = []
        for flw, dly in zip(input_flow, input_delay):
            keys.append((flw, dly / punits))
            etv.append(e_table[flw])
            e_totv.append(e_table[flw] / tot_table[tot_threshold])

        # Create and join the new e_table.
        e_table = pd.DataFrame(etv, index=keys)
        e_table.columns = [new_e_table_name.upper()]
        self._join(new_e_table_name, e_table=e_table)

        # Create and join the new e_table_tot for LIST_OUTPUT.
        e_table_tot = pd.DataFrame(e_totv, index=keys)
        e_table_tot.columns = pd.Index([new_e_table_name.upper()])
        self._join(new_e_table_name, e_table_tot=e_table_tot)

        # This is here only to be able to include the time units in the
        # LIST_OUTPUT.
        self.e_table_metadata[new_e_table_name.upper()] = {
            "exceedance_time_units": exceedance_time_units,
            "source": series_name,
            "under_over": under_over,
        }

    @validate_call
    def flow_duration(
        self,
        series_name: str,
        new_g_table_name: str,
        exceedance_probabilities=(99.5, 99, 98, 95, 90, 75, 50, 25, 10, 5, 2, 1, 0.5),
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        """Calculate the flow duration curve for a time series."""
        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )

        exceedance_probabilities = [float(i) for i in exceedance_probabilities]

        ans = hydrotoolbox.flow_duration(
            input_ts=series,
            exceedance_probabilities=exceedance_probabilities,
        )

        self._join(new_g_table_name, g_table=ans)
        self.g_table_metadata[new_g_table_name.upper()] = {
            "source": series_name,
            "kind": "flow_duration",
            "start_date": series.index[0],
            "end_date": series.index[-1],
        }

    @validate_call
    def hydrologic_indices(
        self,
        series_name: str,
        new_g_table_name: str,
        drainage_area: Union[int, str] = 1,
        use_median: bool = False,
        stream_classification=None,
        flow_component=None,
        ma=None,
        ml=None,
        mh=None,
        fl=None,
        fh=None,
        dl=None,
        dh=None,
        ta=None,
        tl=None,
        th=None,
        ra=None,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
        current_definitions=False,
    ):
        """Calculate hydrologic indices for a time series."""
        mapper = {
            "MA": ma,
            "ML": ml,
            "MH": mh,
            "FL": fl,
            "FH": fh,
            "DL": dl,
            "DH": dh,
            "TA": ta,
            "TL": tl,
            "TH": th,
            "RA": ra,
        }

        ind = []
        for key, value in mapper.items():
            if value is None:
                continue
            value = tsutils.make_list(value, sep=" ")
            ind.extend([f"{key}{i}" for i in value])
        if stream_classification is not None:
            ind.extend(
                [f"{sc}" for sc in tsutils.make_list(stream_classification, sep=" ")]
            )

        if flow_component is not None:
            ind.extend([f"{fc}" for fc in tsutils.make_list(flow_component, sep=" ")])

        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )

        gtab = hydrotoolbox.indices(
            ind, input_ts=series, use_median=use_median, drainage_area=drainage_area
        )
        gtab = pd.DataFrame(gtab, index=[0]).T

        # Undocumented feature of TSPROC: if current_definitions is True, then
        # the g_table gtab is printed to the screen.
        if self._normalize_bools(current_definitions) is True:
            print(gtab)

        self._join(new_g_table_name, g_table=gtab)

        # The g_table_metadata is to collect information for LIST_OUTPUT.
        self.g_table_metadata[new_g_table_name.upper()] = {
            "source": series_name,
            "kind": "hydrologic_indices",
            "start_date": series.index[0],
            "end_date": series.index[-1],
        }

    @validate_call
    def hydro_events(
        self,
        series_name: str,
        new_series_name: str,
        rise_lag: Annotated[int, Field(ge=0)],
        fall_lag: Annotated[int, Field(ge=0)],
        window: Annotated[int, Field(ge=1)] = 1,
        min_peak: Annotated[float, Field(ge=0)] = 0.0,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Calculate hydrologic events for a time series."""
        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )
        series = hydrotoolbox.storm_events(
            rise_lag, fall_lag, input_ts=series, window=window, min_peak=min_peak
        )

        self._join(new_series_name, series=series)

    @validate_call
    def hydro_peaks(
        self,
        series_name: str,
        new_series_name: str,
        window: Annotated[int, Field(ge=0)] = 1,
        min_peak: Annotated[float, Field(ge=0)] = 0.0,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Calculate hydrologic peaks for a time series."""
        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )
        series = hydrotoolbox.storm_events(
            0, 0, input_ts=series, window=window, min_peak=min_peak
        )

        self._join(new_series_name, series=series)

    @validate_call
    def list_output(
        self,
        file,
        series_format: Literal["long", "short", "ssf"] = "",
        series_name=(),
        s_table_name=(),
        c_table_name=(),
        v_table_name=(),
        e_table_name=(),
        g_table_name=(),
        _ins_file="",
    ):
        """
        List the output in the following order:
            - series: list all series
            - s_table: list all s_tables
            - c_table: list all c_tables
            - v_table: list all v_tables
            - e_table: list all e_tables
            - g_table: list all g_tables
        """
        self.last_list_output_parameters["file"] = file
        self.last_list_output_parameters["series_name"] = series_name
        self.last_list_output_parameters["series_format"] = series_format
        self.last_list_output_parameters["s_table_name"] = s_table_name
        self.last_list_output_parameters["c_table_name"] = c_table_name
        self.last_list_output_parameters["v_table_name"] = v_table_name
        self.last_list_output_parameters["e_table_name"] = e_table_name
        self.last_list_output_parameters["g_table_name"] = g_table_name

        if isinstance(series_name, str):
            series_name = [series_name]
        if isinstance(s_table_name, str):
            s_table_name = [s_table_name]
        if isinstance(c_table_name, str):
            c_table_name = [c_table_name]
        if isinstance(v_table_name, str):
            v_table_name = [v_table_name]
        if isinstance(e_table_name, str):
            e_table_name = [e_table_name]
        if isinstance(g_table_name, str):
            g_table_name = [g_table_name]

        if series_name and not series_format:
            raise ValueError(
                tsutils.error_wrapper(
                    """
                    When listing series, the series_format must be specified.
                    """
                )
            )

        self.ofile = _ins_file or file

        fortran_format_data = {
            "table": FortranRecordWriter(r"(t5, a, t55, 1PG14.7, /)"),
            "series_long": FortranRecordWriter(
                r"(1x, a, t20, a10, 3x, a8, 3x, g16.9, /)"
            ),
            "series_short": FortranRecordWriter(r"(4x, g16.9, /)"),
            "series_ssf": FortranRecordWriter(r"(4x, a10, 3x, g16.9, /)"),
            "v_table": FortranRecordWriter(
                r"(t5, 'From ', a10, ' ', a8, ' to ', a10, ' ', a8, '  volume = ', G18.12, /)"
            ),
            "e_table_header": FortranRecordWriter(
                r"(t4, 'Flow', t19, 'Time delay (', a, ')', t40, 'Time ', a, ' (', a, ')', t60, 'Fraction of time ', a, ' threshold', /)"
            ),
            "e_table_row": FortranRecordWriter(
                r"(t4, 'Flow', t19, 'Time delay (', a, ')', t40, 'Time ', a, ' (', a, ')', t60, 'Fraction of time ', a, ' threshold', /)"
            ),
            "g_table_row": FortranRecordWriter(r"(t4, a, t82, g14.7, /)"),
        }
        fortran_format_instructions = {
            "series_long": FortranRecordWriter(r"('l', a, t6, '[', a, a, ']42:65', /)"),
            "series_short": FortranRecordWriter(r"('l', a, t6, '[', a, a, ']2:25', /)"),
            "s_table_row": FortranRecordWriter(r"('l', a, t6, '[', a, a, ']51:69', /)"),
            "g_table_row": FortranRecordWriter(r"('l', a, t6, '[', a, a, ']82:96', /)"),
            "e_table_row": FortranRecordWriter(r"('l', a, t6, '[', a, a, ']59:78', /)"),
        }

        # Time series first
        with open(self.ofile, "w", encoding="utf-8") as fp:
            fp.write("pif $\n" if _ins_file else "")
            for sern in series_name:
                fp.write(
                    "" if _ins_file else f'\n TIME_SERIES "{sern.lower()}" ---->\n'
                )
                series = self._get_series(sern.upper())
                start, end = self.series_dates[sern.upper()]
                line_skip = 3
                for rowno, (index, value) in enumerate(
                    series.loc[start:end].dropna().items()
                ):
                    if series_format == "long":
                        date = index.strftime(self.date_format)
                        time = index.strftime("%H:%M:%S")
                        if series.index.freqstr == "D":
                            time = "12:00:00"
                        fp.write(
                            fortran_format_instructions["series_long"].write(
                                [f"{line_skip}", sern.lower(), f"{rowno+1}"]
                            )
                            if _ins_file
                            else fortran_format_data["series_long"].write(
                                [sern.lower(), date, time, value]
                            )
                        )
                    elif series_format == "short":
                        fp.write(
                            fortran_format_instructions["series_short"].write(
                                [f"{line_skip}", sern, f"{rowno+1}"]
                            )
                            if _ins_file
                            else fortran_format_data["series_short"].write([value])
                        )
                    elif series_format == "ssf":
                        fp.write(
                            fortran_format_instructions["series_long"].write(
                                [f"{line_skip}", sern, f"{rowno+1}"]
                            )
                            if _ins_file
                            else fortran_format_data["series_ssf"].write([sern, value])
                        )
                    line_skip = 1

            for s_tab in s_table_name:
                st = self._get_s_table(s_tab.upper()).dropna()
                stab_meta = self.s_table_metadata[s_tab.upper()]
                fp.write(
                    "l8\n"
                    if _ins_file
                    else f"""
 S_TABLE "{s_tab}" ---->
     Series for which data calculated:                 "{stab_meta["source_name"].lower()}"
     Starting date for data accumulation:              {stab_meta["start_date"].strftime("%Y-%m-%d")}
     Ending date for data accumulation                 {stab_meta["end_date"].strftime("%Y-%m-%d")}
     Logarithmic transformation of series?             {stab_meta["log_transformed"]}
     Exponent in power transformation:                 {stab_meta["exponent"]}
"""
                )
                for index, value in st.items():
                    fp.write(
                        "1l {s_tab}  \n"
                        if _ins_file
                        else fortran_format_data["table"].write([index, value])
                    )

            for c_tab in c_table_name:
                stats = self._get_c_table(c_tab).dropna()
                ctab = self.c_table_metadata[c_tab.upper()]
                fp.write(
                    "l9\n"
                    if _ins_file
                    else f"""
 C_TABLE "{c_tab}" ---->
    Observation time series name:                     "{ctab['obs_name'].lower()}"
    Simulation time series name:                      "{ctab['sim_name'].lower()}"
    Beginning date of series comparison:              {ctab["start_date"].strftime("%Y-%m-%d")}
    Beginning time of series comparison:              {ctab["start_date"].strftime("%H:%M:%S")}
    Finishing date of series comparison:              {ctab["end_date"].strftime("%Y-%m-%d")}
    Finishing time of series comparison:              {ctab["end_date"].strftime("%H:%M:%S")}
    Number of series terms in this interval:          {ctab["num_terms"]}
"""
                )
                for index, value in stats.items():
                    fp.write(
                        fortran_format_instructions["table"].write([index, value])
                        if _ins_file
                        else fortran_format_data["table"].write([index, value])
                    )

            for v_tab in v_table_name:
                fp.write(
                    "l4"
                    if _ins_file
                    else f"""
 V_TABLE "{v_tab}" ---->
     Volumes calculated from series "{self.v_table_metadata[v_tab.upper()]["source_name"]}" are as follows:-
"""
                )
                v_table = self._get_v_table(v_tab)
                for index, value in v_table.dropna().items():
                    fp.write(
                        "v_table"
                        if _ins_file
                        else fortran_format_data["v_table"].write(
                            [
                                index[0].strftime(self.date_format),
                                index[0].strftime("%H:%M:%S"),
                                index[1].strftime(self.date_format),
                                index[1].strftime("%H:%M:%S"),
                                value,
                            ]
                        )
                    )

            for e_tab_name in e_table_name:
                e_tab = self._get_e_table(e_tab_name.upper())
                et_tot = self.e_table_tot[e_tab_name.upper()]
                etab_meta = self.e_table_metadata[e_tab_name.upper()]
                units = etab_meta["exceedance_time_units"]
                direction = {"over": "above", "under": "below"}[etab_meta["under_over"]]
                fp.write(
                    ""
                    if _ins_file
                    else f"""
 E_TABLE "{e_tab_name.lower()}" ---->
"""
                )
                fp.write(
                    ""
                    if _ins_file
                    else fortran_format_data["e_table_header"].write(
                        [units, direction, units, direction]
                    )
                )
                line_skip = 4
                for row, (index, value) in enumerate(e_tab.dropna().items(), start=1):
                    fp.write(
                        fortran_format_instructions["e_table_row"].write(
                            [f"{line_skip}", e_tab_name.lower(), f"{row}"]
                        )
                        if _ins_file
                        else FortranRecordWriter(
                            "t2, g14.7, t20, g14.7, t40, g14.7, t63, g14.7, /"
                        ).write([index[0], index[1], value, et_tot[index]])
                    )
                    line_skip = 1

            for g_tab in g_table_name:
                src = self.g_table_metadata[g_tab.upper()]["source"]
                kind = self.g_table_metadata[g_tab.upper()]["kind"]
                start_date = self.g_table_metadata[g_tab.upper()]["start_date"]
                end_date = self.g_table_metadata[g_tab.upper()]["end_date"]

                g_table = self.g_table[g_tab.upper()].dropna()
                sindex = sorted(g_table.index, key=natural_keys)
                g_table = g_table.loc[sindex]

                fp.write(
                    ""
                    if _ins_file
                    else f"""
 G_TABLE "{g_tab}" ---->
"""
                )

                if kind == "flow_duration":
                    #  G_TABLE "mduration" ---->
                    #    Flow-duration curve for series "mflow" (11/08/8672 to 11/07/8693)                     Value
                    #    99.50% of flows exceed:                                                         4.912684
                    fp.write(
                        ""
                        if _ins_file
                        else FortranRecordWriter(
                            "t4, 'Flow duration curve for ', a, ' ', a, ':', a, t85, 'Value', /"
                        ).write(
                            [
                                src,
                                start_date.strftime("%m/%d/%Y"),
                                end_date.strftime("%m/%d/%Y"),
                            ]
                        )
                    )

                    line_skip = 4
                    g_table = g_table.sort_index(ascending=False)
                    for row, (index, value) in enumerate(
                        g_table.dropna().items(), start=1
                    ):
                        fp.write(
                            fortran_format_instructions["g_table_row"].write(
                                [f"{line_skip}", g_tab.lower(), f"{row}"]
                            )
                            if _ins_file
                            else fortran_format_data["g_table_row"].write(
                                [f"{index:>6.02%} of flows exceed:", value]
                            )
                        )
                        line_skip = 1
                elif kind == "hydrologic_indices":
                    #  G_TABLE "mduration_p" ---->
                    #    Hydrologic Index and description (Olden and Poff, 2003)                               Value
                    #    MA16: Mean monthly flow May-Aug:                                                      4.912684
                    fp.write(
                        ""
                        if _ins_file
                        else FortranRecordWriter(
                            "t4, 'Hydrologic index for ', a, ' ', a, ':', a, t85, 'Value', /"
                        ).write(
                            [
                                src,
                                start_date.strftime("%m/%d/%Y"),
                                end_date.strftime("%m/%d/%Y"),
                            ]
                        )
                    )

                    line_skip = 4
                    for row, (index, value) in enumerate(g_table.items(), start=1):
                        fp.write(
                            fortran_format_instructions["g_table_row"].write(
                                [f"{line_skip}", g_tab.lower(), f"{row}"]
                            )
                            if _ins_file
                            else FortranRecordWriter("t4, a, t82, g14.7, /").write(
                                [index, value]
                            )
                        )
                        line_skip = 1

    @validate_call
    def move(
        self,
        new_entity_name: str,
        series_name: str = "",
        v_table_name: str = "",
        c_table_name: str = "",
        s_table_name: str = "",
        e_table_name: str = "",
        g_table_name: str = "",
        overwrite: Union[bool, Literal["yes", "no"]] = False,
    ):
        """Move an entity."""
        overwrite = self._normalize_bools(overwrite)

        self.copy(
            new_entity_name,
            series_name=series_name,
            v_table_name=v_table_name,
            c_table_name=c_table_name,
            s_table_name=s_table_name,
            e_table_name=e_table_name,
            g_table_name=g_table_name,
            overwrite=overwrite,
        )
        self.erase_entity(
            series_name=series_name,
            v_table_name=v_table_name,
            c_table_name=c_table_name,
            s_table_name=s_table_name,
            e_table_name=e_table_name,
            g_table_name=g_table_name,
        )

    @validate_call
    def new_series_uniform(
        self,
        new_series_name: str,
        new_series_value: float,
        time_interval: int,
        time_unit: str,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Create a new time series with a uniform value and interval."""
        ptunit = {
            "seconds": "S",
            "minutes": "T",
            "hours": "H",
            "days": "D",
            "months": "M",
            "years": "Y",
        }.get(time_unit, time_unit)
        ptunit = f"{time_interval}{ptunit}"
        ndr = pd.date_range(
            start=self._normalize_datetimes(date_1, time_1),
            end=self._normalize_datetimes(date_2, time_2),
            freq=ptunit,
        )
        ndf = pd.DataFrame(data=[new_series_value] * len(ndr), index=ndr)
        self._join(new_series_name, series=ndf)

    @validate_call
    def new_time_base(
        self, series_name: str, new_series_name: str, tb_series_name: str
    ):
        """Create a new time series with a new time base."""
        tb_series = pd.DataFrame(self._get_series(tb_series_name)).dropna()
        series = pd.DataFrame(self._get_series(series_name))
        nseries = tb_series.join(series, how="outer").iloc[:, 1].astype(float)
        nseries = nseries.interpolate(method="time", limit_direction="both").loc[
            tb_series.index
        ]
        self._join(new_series_name, series=nseries)

    @validate_call
    def period_statistics(
        self,
        series_name: str,
        new_series_name: str,
        statistic: Literal[
            "mean", "std_dev", "median", "sum", "maximum", "minimum", "range"
        ],
        period: Literal["month_many", "month_one", "year"],
        time_abscissa: Literal["start", "end", "middle"],
        year_type: Literal["water_high", "water_low", "calendar"] = "water_high",
        log: bool = False,
        power: float = 1,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Calculate period statistics for a time series."""
        series = self._prepare_series(
            series_name,
            log=log,
            power=power,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )

        method = {"std_dev": "std", "maximum": "max", "minimum": "min"}.get(
            statistic, statistic
        )

        tab = ""
        if time_abscissa == "start":
            tab = "S"
        elif time_abscissa == "end":
            tab = "E"

        wyt = ""
        if year_type == "water_high":
            wyt = "-SEP"
        elif year_type == "water_low":
            wyt = "-MAR"
        elif year_type == "calendar":
            wyt = "-DEC"

        if period == "month_many":
            series = series.resample(f"M{tab}").agg(method)
        elif period == "month_one":
            series = series.groupby(lambda x: x.month).agg(method)
            series.index = pd.date_range(start="1900-01-01", end="1900-12-31", freq="M")
        elif period == "year":
            series = series.resample(f"A{tab}{wyt}").agg(method)
        self._join(new_series_name, series=series)

    @validate_call
    def plot(self, series_name: Union[str, list], file: str, **kwargs):
        """Plot a time series."""
        if isinstance(series_name, str):
            series_name = [series_name]
        date_1 = kwargs.pop("date_1", None)
        time_1 = kwargs.pop("time_1", None)
        date_2 = kwargs.pop("date_2", None)
        time_2 = kwargs.pop("time_2", None)

        series = pd.DataFrame()
        for prepared in [
            self._prepare_series(
                sn,
                date_1=date_1,
                time_1=time_1,
                date_2=date_2,
                time_2=time_2,
            )
            for sn in series_name
        ]:
            series = series.join(prepared, how="outer")

        if isinstance(kwargs.get("secondary_y", None), str):
            kwargs["secondary_y"] = [kwargs["secondary_y"].upper()]
        elif isinstance(kwargs.get("secondary_y", None), (list, tuple)):
            kwargs["secondary_y"] = [sn.upper() for sn in kwargs["secondary_y"]]

        kwargs["xlim"] = (kwargs.pop("xlim_min", None), kwargs.pop("xlim_max", None))
        kwargs["ylim"] = (kwargs.pop("ylim_min", None), kwargs.pop("ylim_max", None))

        kwargs["figsize"] = (
            kwargs.pop("figsize_width", 10),
            kwargs.pop("figsize_height", 6),
        )

        kwargs["legend"] = self._normalize_bools(kwargs["legend"])
        kwargs["logx"] = self._normalize_bools(kwargs["logx"])
        kwargs["logy"] = self._normalize_bools(kwargs["logy"])
        kwargs["mark_right"] = self._normalize_bools(kwargs["mark_right"])
        kwargs["grid"] = self._normalize_bools(kwargs["grid"])

        ax = series.plot(**kwargs)
        ax.figure.savefig(file)
        plt.close(ax.figure)

    @validate_call
    def reduce_time_span(
        self,
        series_name: str,
        new_series_name: str,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Reduce the time span of a time series."""
        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )
        self._join(new_series_name, series=series)

    def settings(self, date_format="%Y-%m-%d"):
        """Set the date format for the output."""
        if date_format == "dd/mm/yyyy":
            date_format = "%d/%m/%Y"
        if date_format == "mm/dd/yyyy":
            date_format = "%m/%d/%Y"
        self.date_format = date_format

    @validate_call
    def v_table_to_series(
        self,
        new_series_name,
        v_table_name,
        time_abscissa,
    ):
        """Create a new time series from a v_table."""
        v_table = self._get_v_table(v_table_name)
        if time_abscissa == "start":
            series = v_table.set_index("start").drop("end", axis="columns")
        elif time_abscissa == "end":
            series = v_table.set_index("end").drop("start", axis="columns")
        self._join(new_series_name, series=series)

    @validate_call
    def volume_calculation(
        self,
        series_name,
        new_v_table_name,
        flow_time_units,
        date_file=None,
        automatic_dates=None,
        factor=1,
    ):
        """Calculate the volume of a time series.

        flow_time_units is the time units of the flow series and is only necessary
        if running with TSPROC.  For tsblender, it is ignored since the time units
        are already known because they are in the dataframe.
        """
        if ((date_file is None) and (automatic_dates is None)) or (
            (date_file is not None) and (automatic_dates is not None)
        ):
            raise ValueError(
                tsutils.error_wrapper(
                    """
                    Must supply one of "DATE_FILE" or "AUTOMATIC_DATES" keyword
                    options.
                    """
                )
            )

        series = self._get_series(series_name).dropna()

        if automatic_dates:
            mapping = {"year": "AS", "month": "MS", "day": "D"}
            start_end = pd.date_range(
                start=series.index[0],
                end=series.index[-1],
                freq=mapping[automatic_dates],
            ).to_period()
            start_end = [[i.start_time, i.end_time] for i in start_end]
        if date_file:
            start_end = []
            with open(date_file, encoding="ascii") as fpi:
                for line in fpi:
                    words = line.strip().split()
                    start_end.append(
                        [
                            self._normalize_datetimes(words[0], words[1]),
                            self._normalize_datetimes(words[2], words[3]),
                        ]
                    )

        volume = []
        for start, end in start_end:
            nsend = pd.DatetimeIndex([start, end])
            dfi = pd.DataFrame(
                data=np.interp(
                    nsend, series.index, np.array(series.values).astype(np.float64)
                ),
                index=nsend,
            )
            mask = (series.index > start) & (series.index < end)
            dfi = dfi.merge(
                series[mask], how="outer", left_index=True, right_index=True
            )
            dfi.iloc[0, 1] = dfi.iloc[0, 0]
            dfi.iloc[-1, 1] = dfi.iloc[-1, 0]
            dfi = pd.Series(dfi.iloc[:, 1])
            dfindex = dfi.index.astype(np.int64) // 10**9
            volume.append(np.trapz(dfi, x=dfindex) * float(factor))
        start = [pd.to_datetime(i[0]) for i in start_end]
        end = [pd.to_datetime(i[1]) for i in start_end]
        series = pd.DataFrame(zip(start, end, volume))
        series = series.set_index([0, 1])
        series.index.names = ["start", "end"]
        series.columns = [f"{series_name}"]
        self._join(new_v_table_name, v_table=series)
        self.v_table_metadata[new_v_table_name.upper()] = {
            "source_name": series_name,
        }

    @validate_call
    def usgs_hysep(
        self,
        series_name: str,
        new_series_name: str,
        hysep_type: str,
        time_interval: int,
        area: float = 1.0,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        """Perform a USGS HYSEP baseflow separation."""
        series = self._prepare_series(
            series_name,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )
        if time_interval is None:
            time_interval = 5
        if hysep_type.lower() == "fixed":
            series = hydrotoolbox.baseflow_sep.usgs_hysep_fixed(series, time_interval)
        elif hysep_type.lower() == "local":
            series = hydrotoolbox.baseflow_sep.usgs_hysep_local(series, time_interval)
        elif hysep_type.lower() == "sliding":
            series = hydrotoolbox.baseflow_sep.usgs_hysep_slide(series, time_interval)

        self._join(new_series_name, series=series)

    def _write_pest_file_table(
        self, obs_table_name, mod_table_name, obs_weight, get_function, obs_min_max=""
    ):
        observation_data = []
        if isinstance(obs_table_name, str):
            obs_table_name = [obs_table_name]
        if isinstance(mod_table_name, str):
            mod_table_name = [mod_table_name]
        if isinstance(obs_weight, str):
            obs_weight = [obs_weight]
        if isinstance(obs_min_max, str):
            obs_min_max = [obs_min_max]

        nobs_min_max = []
        for min_max in obs_min_max:
            words = min_max.split() if isinstance(min_max, str) else min_max
            minval = None
            maxval = None
            if len(words) == 2:
                minval, maxval = (float(i) for i in words)
                if minval > maxval:
                    raise ValueError(
                        tsutils.error_wrapper(
                            f"""
                            The minimum value {minval} is greater than the
                            maximum value {maxval}.
                            """
                        )
                    )
                minval = max(minval, 0)
            nobs_min_max.append((minval, maxval))

        for obs, model, weight_equation, min_max in zip(
            obs_table_name,
            mod_table_name,
            obs_weight,
            nobs_min_max,
        ):
            weights = self._series_equation(
                weight_equation.replace("@_abs_value", f"abs({obs.upper()})")
            )
            obsval = get_function(obs)
            weights_df = obsval.copy()
            weights_df[:] = weights
            if min_max:
                lower, upper = min_max
            else:
                lower = 0
                upper = None
            weights_df = weights_df.clip(
                lower=lower,
                upper=upper,
            )
            for index, (val, weight) in enumerate(
                zip(obsval.dropna(), weights_df.dropna())
            ):
                modindex = f"{model.lower()}{index+1}"
                observation_data.append(
                    f"{modindex:20} {val:15f} {weight:15f} {model.lower():20}"
                )
        return observation_data

    @validate_call
    def write_pest_files(
        self,
        new_pest_control_file,
        new_instruction_file,
        template_file=None,
        model_input_file=None,
        parameter_data_file=None,
        parameter_group_file=None,
        observation_series_name=(),
        model_series_name=(),
        series_weights_equation=(),
        series_weights_min_max=(),
        observation_s_table_name=(),
        model_s_table_name=(),
        s_table_weights_equation=(),
        s_table_weights_min_max=(),
        observation_v_table_name=(),
        model_v_table_name=(),
        v_table_weights_equation=(),
        v_table_weights_min_max=(),
        observation_e_table_name=(),
        model_e_table_name=(),
        e_table_weights_equation=(),
        e_table_weights_min_max=(),
        observation_g_table_name=(),
        model_g_table_name=(),
        g_table_weights_equation=(),
        g_table_weights_min_max=(),
        automatic_user_intervention=None,
        truncated_svd=None,
        model_command_line=None,
        **kwds,
    ):
        """Write PEST control file and instruction file."""
        self.last_list_output_parameters["_ins_file"] = new_instruction_file
        self.list_output(**self.last_list_output_parameters)

        # Automatic user intervention and SVD truncation have to be handled
        # first because they set different defaults for other keywords.
        _doaui = kwds.get("doaui", "aui").lower()
        if _doaui not in ["aui", "auid", "noaui"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "doaui" must be either "aui", "auid", or
                    "noaui", not "{_doaui}".
                    """
                )
            )
        if automatic_user_intervention is not None:
            warning(
                """
                The "automatic_user_intervention" keyword is unused and replaced
                with setting the "doaui" keyword to "aui", "auid", or "noaui".
                """
            )
        if truncated_svd is not None:
            warning(
                """
                The "truncated_svd" keyword is unused and replaced with
                setting the "eigthresh" keyword to a small positive value.
                """
            )
        eigthresh = kwds.get("eigthresh", 0)
        if eigthresh > 0 and _doaui in ["aui", "auid"]:
            raise ValueError(
                tsutils.error_wrapper(
                    """
                    If the "eigthresh" keyword is set to a positive value,
                    that activates the truncated SVD option and the "doaui"
                    keyword must be set to "noaui".
                    """
                )
            )

        template_parameters = set()
        if isinstance(template_file, str):
            template_file = [template_file]
        ntplfle = len(template_file)
        for tpl in template_file:
            with open(tpl, encoding="ascii") as fpi:
                ptf, marker = fpi.readline().split()
                if ptf != "ptf":
                    raise ValueError(
                        tsutils.error_wrapper(
                            f"""
                            The template file "{tpl}" does not start with a
                            "ptf" instruction.
                            """
                        )
                    )
                for line in fpi:
                    for match in re.finditer(f"{marker}(\\w+?){marker}", line):
                        template_parameters.add(match.group(0))

        parameter_groups = ["", "* parameter groups"]

        npargp = 0
        parameter_group_names = set()
        for line, line_number in self._read_file(parameter_group_file):
            npargp += 1
            words = line.split()
            if len(words) == 7:
                pargpnme, inctyp, derinc, derinclb, forcen, derincmul, dermthd = words
                splits = ""
            elif len(words) == 10:
                (
                    pargpnme,
                    inctyp,
                    derinc,
                    derinclb,
                    forcen,
                    derincmul,
                    dermthd,
                    splitthresh,
                    splitreldiff,
                    splitaction,
                ) = words
                splits = f"{splitthresh:>10} {splitreldiff:>10} {splitaction:>10}"
            else:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_group_file}" has
                        {len(words)} items.  It must have either 7 or 10 items.
                        """
                    )
                )
            if pargpnme.lower() in parameter_group_names:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_group_file}" has
                        a duplicate parameter group name "{pargpnme}".
                        """
                    )
                )
            parameter_group_names.add(pargpnme.lower())
            inctype = inctyp.lower()
            if inctype not in ["relative", "absolute", "rel_to_max"]:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_group_file}" has
                        an incorrect type of increment.  It must be either
                        "relative", "absolute", or "rel_to_max" instead of
                        "{inctype}".
                        """
                    )
                )
            derinc = float(derinc)
            derinclb = float(derinclb)
            forcen = forcen.lower()
            if forcen not in ["always_2", "always_3", "always_5", "switch", "switch_5"]:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_group_file}" has
                        an incorrect type of forcing.  It must be either
                        "always_2", "always_3", "always_5", "switch", or
                        "switch_5" instead of "{forcen}".
                        """
                    )
                )
            derincmul = float(derincmul)
            dermthd = dermthd.lower()
            if dermthd not in [
                "parabolic",
                "best_fit",
                "outside_pts",
                "minvar",
                "maxprec",
            ]:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_group_file}" has
                        an incorrect type of derivative method.  It must be
                        either "parabolic", "best_fit", "outside_pts",
                        "minvar", or "maxprec" instead of "{dermthd}".
                        """
                    )
                )
            parameter_groups.append(
                f"{pargpnme:<12} {inctyp:>10} {derinc:>10} {derinclb:>10} {forcen:>10} {derincmul:>10} {dermthd:>11} {splits}"
            )
        parameter_groups = "\n".join(parameter_groups)

        parameter_data = ["", "* parameter data"]
        tied_parameters = OrderedDict()
        npar = 0
        nequation = 0
        equations = []
        secondary_equations = set()
        for line, line_number in self._read_file(parameter_data_file):
            if "=" in line:
                words = line.split("=")
                secondary_equations.add(words[0].strip())
                equations.append(
                    f"{words[0].strip():<15} = {words[1].replace(' ', '').strip()}"
                )
                nequation += 1
                continue
            else:
                npar += 1
            (
                parnme,
                partrans,
                parchglim,
                parval1,
                parlbnd,
                parubnd,
                pargp,
                scale,
                offset,
                dercom,
            ) = line.split()
            if len(parnme) > 12:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        Line {line_number} of "{parameter_data_file}" has
                        a parameter name that is longer than 12 characters.
                        """
                    )
                )
            if parnme.lower() == "none":
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The {npar + 1} parameter name in data file
                        "{parameter_data_file}" cannot be "none".
                        """
                    )
                )
            if partrans.lower() not in ["fixed", "tied", "log", "none"]:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The parameter transformation in data file
                        "{parameter_data_file}" must be either "fixed",
                        "tied_*", "log", or "none", not "{partrans}".
                        """
                    )
                )
            if parchglim.lower()[:8] not in ["factor", "relative", "absolute"]:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The {npar + 1} parameter change limit in data file
                        "{parameter_data_file}" must be either "factor",
                        "relative", or "absolute(N)", not "{parchglim}".
                        """
                    )
                )
            if pargp.lower() not in parameter_group_names:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The parameter data file has a parameter group name
                        "{pargp}" that is not in the parameter group file.
                        """
                    )
                )
            parval1 = float(parval1)
            parlbnd = float(parlbnd)
            parubnd = float(parubnd)
            if parval1 < parlbnd or parval1 > parubnd:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The {npar + 1} parameter value {parval1} in data file
                        "{parameter_data_file}" must be between the lower and
                        upper bounds.
                        """
                    )
                )
            if partrans.lower()[:4] == "tied":
                _, tiedto = partrans.split("_")
                tied_parameters[parnme] = tiedto
                partrans = "tied"

            parameter_data.append(
                f"{parnme.lower():<15} {partrans.lower():>10} {parchglim.lower():>15} {parval1:>15f} {parlbnd:>15f} {parubnd:>15f} {pargp:>10} {scale:>10} {offset:>10} {dercom:>10}"
            )

        control_data_equations = ""
        if equations:
            parameter_data.extend(equations)
            control_data_equations = (
                f"nparsec={len(secondary_equations)} nequation={nequation}"
            )

        for parnme, tiedto in tied_parameters.items():
            parameter_data.append(f"{parnme:<15} {tiedto:>10}")

        # Have to read the series and tables to get the number of observations
        # and observation groups here even though they are written later.

        nobsgp = 0
        observation_groups = """
* observation groups"""
        for obsgrp_name in [
            model_series_name,
            model_s_table_name,
            model_v_table_name,
            model_e_table_name,
            model_g_table_name,
        ]:
            if obsgrp_name is not None:
                if isinstance(obsgrp_name, str):
                    obsgrp_name = [obsgrp_name]
                for obsgrp in obsgrp_name:
                    observation_groups += f"\n{obsgrp.lower()}"
                    nobsgp += 1

        observation_data = ["", "* observation data"]

        nobs = 0
        series_data = self._write_pest_file_table(
            observation_series_name,
            model_series_name,
            series_weights_equation,
            self._get_series,
            series_weights_min_max,
        )
        nobs += len(series_data)
        observation_data.extend(series_data)

        s_table_data = self._write_pest_file_table(
            observation_s_table_name,
            model_s_table_name,
            s_table_weights_equation,
            self._get_s_table,
            s_table_weights_min_max,
        )
        nobs += len(s_table_data)
        observation_data.extend(s_table_data)

        v_table_data = self._write_pest_file_table(
            observation_v_table_name,
            model_v_table_name,
            v_table_weights_equation,
            self._get_v_table,
            v_table_weights_min_max,
        )
        nobs += len(v_table_data)
        observation_data.extend(v_table_data)

        e_table_data = self._write_pest_file_table(
            observation_e_table_name,
            model_e_table_name,
            e_table_weights_equation,
            self._get_e_table,
            e_table_weights_min_max,
        )
        nobs += len(e_table_data)
        observation_data.extend(e_table_data)

        g_table_data = self._write_pest_file_table(
            observation_g_table_name,
            model_g_table_name,
            g_table_weights_equation,
            self._get_g_table,
            g_table_weights_min_max,
        )
        nobs += len(g_table_data)
        observation_data.extend(g_table_data)

        observation_data = "\n".join(observation_data)

        nprior = 0

        # TSPROC has only one instruction file.
        ninsfle = 1

        # Second line of the control file
        rstfle = kwds.get("rstfle", "restart").lower()
        if rstfle not in ["restart", "norestart"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "rstfle" must be either "restart" or
                    "norestart", not "{rstfle}"
                    """
                )
            )
        pestmode = kwds.get("pestmode", "estimation").lower()
        if pestmode not in ["estimation", "pareto", "prediction", "regularization"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "pestmode" must be either "estimation",
                    "pareto", "prediction", or "regularization", not
                    "{pestmode}"
                    """
                )
            )

        # Third line of the control file
        # npar calculated by reading the parameter data file
        # nobs calculated by reading the series and tables that are part of
        #     list_output
        # npargp calculated by reading the parameter group file
        # nprior read from the parameter data file
        # nobsgp number of observation groups from the series and tables that
        #     are part of list_output
        _maxcompdim = kwds.get("maxcompdim", "")
        _derzerolim = kwds.get("derzerolim", "")

        # Fourth line of the control file
        # ntplfle count of template_file entries in write_pest_files
        # ninsfle count of new_instruction_file entries in write_pest_files
        precis = kwds.get("precis", "single").lower()
        if precis not in ["single", "double"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "precis" must be either "single" or
                    "double", not "{precis}"
                    """
                )
            )
        dpoint = kwds.get("dpoint", "point").lower()
        if dpoint not in ["point", "nopoint"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "dpoint" must be either "point" or
                    "nopoint", not "{dpoint}"
                    """
                )
            )
        _numcom = kwds.get("numcom", 1)
        _jacfile = kwds.get("jacfile", 0)
        _messfile = kwds.get("messfile", 0)
        _obsreref = kwds.get("obsreref", "noobsreref").lower()
        if _obsreref not in ["obsreref", "noobsreref"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "obsreref" must be either "obsreref" or
                    "noobsreref", not "{_obsreref}"
                    """
                )
            )

        # Fifth line of the control file
        rlambda1 = float(kwds.get("rlambda1", 10.0))
        rlamfac = (
            float(kwds.get("rlamfac", -3.0))
            if eigthresh > 0
            else float(kwds.get("rlamfac", 2.0))
        )
        phiratsuf = float(kwds.get("phiratsuf", 0.3))
        phiredlam = float(kwds.get("phiredlam", 0.03))
        numlam = (
            int(kwds.get("numlam", 1)) if eigthresh > 0 else int(kwds.get("numlam", 10))
        )
        _jacupdate = int(kwds.get("jacupdate", 999))
        _lamforgive = kwds.get("lamforgive", "nolamforgive").lower()
        if _lamforgive not in ["lamforgive", "nolamforgive"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "lamforgive" must be either "lamforgive" or
                    "nolamforgive", not "{_lamforgive}"
                    """
                )
            )
        _derforgive = kwds.get("derforgive", "noderforgive").lower()
        if _derforgive not in ["derforgive", "noderforgive"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "derforgive" must be either "derforgive" or
                    "noderforgive", not "{_derforgive}"
                    """
                )
            )

        # Sixth line of the control file
        relparmax = float(kwds.get("relparmax", 5.0))
        facparmax = float(kwds.get("facparmax", 5.0))
        facorig = float(kwds.get("facorig", 1.0e-3))
        _absparmax = OrderedDict()
        _absparmax[1] = kwds.get("absparmax_1", None)
        _absparmax[2] = kwds.get("absparmax_2", None)
        _absparmax[3] = kwds.get("absparmax_3", None)
        _absparmax[4] = kwds.get("absparmax_4", None)
        _absparmax[5] = kwds.get("absparmax_5", None)
        _absparmax[6] = kwds.get("absparmax_6", None)
        _absparmax[7] = kwds.get("absparmax_7", None)
        _absparmax[8] = kwds.get("absparmax_8", None)
        _absparmax[9] = kwds.get("absparmax_9", None)
        _absparmax[10] = kwds.get("absparmax_10", None)
        _absparmax = [
            f"absparmax({key})={value}" for key, value in _absparmax.items() if value
        ]
        _iboundstick = int(kwds.get("iboundstick", 0))
        _upvecbend = int(kwds.get("upvecbend", 0))

        # Seventh line of the control file
        phiredswh = float(kwds.get("phiredswh", 0.1))
        _noptswitch = int(kwds.get("noptswitch", 1))
        _splitswh = int(kwds.get("splitswh", 0))

        _dosenreuse = kwds.get("dosenreuse", "nosenreuse").lower()
        if _dosenreuse not in ["senreuse", "nosenreuse"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "dosenreuse" must be either "senreuse" or
                    "nosenreuse", not "{_dosenreuse}"
                    """
                )
            )
        _boundscale = kwds.get("boundscale", "boundscale").lower()
        if _boundscale not in ["boundscale", "noboundscale"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "boundscale" must be either "boundscale" or
                    "noboundscale", not "{_boundscale}"
                    """
                )
            )

        # Eighth line of the control file
        noptmax = int(kwds.get("noptmax", 30))
        phiredstp = float(kwds.get("phiredstp", 0.005))
        nphistp = int(kwds.get("nphistp", 4))
        nphinored = int(kwds.get("nphinored", 4))
        relparstp = float(kwds.get("relparstp", 0.005))
        nrelpar = int(kwds.get("nrelpar", 4))
        _phistopthresh = float(kwds.get("phistopthresh", 0))
        _lastrun = int(kwds.get("lastrun", 1))
        _phiabandon = float(kwds.get("phiabandon", -1.0))

        # Ninth line of the control file
        icov = int(kwds.get("icov", 1))
        icor = int(kwds.get("icor", 1))
        ieig = int(kwds.get("ieig", 1))
        _ires = int(kwds.get("ires", 0))
        _jcosave = kwds.get("jcosave", "jcosave").lower()
        if _jcosave not in ["jcosave", "nojcosave"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "jcosave" must be either "jcosave" or
                    "nojcosave", not "{_jcosave}"
                    """
                )
            )
        _verboserec = kwds.get("verboserec", "verboserec").lower()
        if _verboserec not in ["verboserec", "noverboserec"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "verboserec" must be either "verboserec" or
                    "noverboserec", not "{_verboserec}"
                    """
                )
            )
        _jcosaveitn = kwds.get("jcosaveitn", "nojcosaveitn").lower()
        if _jcosaveitn not in ["jcosaveitn", "nojcosaveitn"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "jcosaveitn" must be either "jcosaveitn" or
                    "nojcosaveitn", not "{_jcosaveitn}"
                    """
                )
            )
        _reisaveitn = kwds.get("reisaveitn", "reisaveitn").lower()
        if _reisaveitn not in ["reisaveitn", "noreisaveitn"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "reisaveitn" must be either "reisaveitn" or
                    "noreisaveitn", not "{_reisaveitn}"
                    """
                )
            )
        _parsaveitn = kwds.get("parsaveitn", "noparsaveitn").lower()
        if _parsaveitn not in ["parsaveitn", "noparsaveitn"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "parsaveitn" must be either "parsaveitn" or
                    "noparsaveitn", not "{_parsaveitn}"
                    """
                )
            )
        _parsaverun = kwds.get("parsaverun", "noparsaverun").lower()
        if _parsaverun not in ["parsaverun", "noparsaverun"]:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The value for "parsaverun" must be either "parsaverun" or
                    "noparsaverun", not "{_parsaverun}"
                    """
                )
            )

        control_data = f"""\
pcf
* control data
{rstfle} {pestmode}
{npar:>10d} {nobs:>10d} {npargp:>10d} {nprior:>10d} {nobsgp:>10d} {_maxcompdim:>10} {_derzerolim:>10} {control_data_equations}
{ntplfle:>10d} {ninsfle:>10d} {precis:>10} {dpoint:>10} {_numcom:>10d} {_jacfile:>10d} {_messfile:>10d} {_obsreref:>10}
{rlambda1:>10f} {rlamfac:>10f} {phiratsuf:>10f} {phiredlam:>10f} {numlam:>10d} {_jacupdate:>10} {_lamforgive:>10} {_derforgive:>10}
{relparmax:>10f} {facparmax:>10f} {facorig:>10f} {_iboundstick:>10d} {_upvecbend:>10d} {" ".join(_absparmax)}
{phiredswh:>10f} {_noptswitch:>10d} {_splitswh:>10d} {_doaui:>10} {_dosenreuse:>10} {_boundscale:>10}
{noptmax:>10d} {phiredstp:>10f} {nphistp:>10d} {nphinored:>10d} {relparstp:>10f} {nrelpar:>10d} {_phistopthresh:>10} {_lastrun:>10} {_phiabandon:>10}
{icov:>10d} {icor:>10d} {ieig:>10d} {_ires:>10} {_jcosave:>10} {_verboserec:>10} {_jcosaveitn:>10} {_reisaveitn:>10} {_parsaveitn:>10} {_parsaverun:>10}"""

        svdmode = int(kwds.get("svdmode", 1))
        maxsing = int(kwds.get("maxsing", npar))
        eigwrite = int(kwds.get("eigwrite", 1))

        singular_value_decomposition = f"""
* singular value decomposition
{svdmode:>10d}
{maxsing:>10d} {eigthresh:>15f}
{eigwrite:>10d}"""

        lsqrmode = int(kwds.get("lsqrmode", 1))
        lsqr_atol = float(kwds.get("lsqr_atol", 1.0e-4))
        lsqr_btol = float(kwds.get("lsqr_btol", 1.0e-4))
        lsqr_conlim = float(kwds.get("lsqr_conlim", 1000))
        lsqr_itnlim = int(kwds.get("lsqr_itnlim", 4 * npar))
        lsqrwrite = int(kwds.get("lsqrwrite", 0))

        lsqr = f"""
* lsqr
{lsqrmode:>10}
{lsqr_atol:>10} {lsqr_btol:>10} {lsqr_conlim:>10} {lsqr_itnlim:>10}
{lsqrwrite:>10}"""

        with open(new_pest_control_file, "w", encoding="ascii") as fpo:
            fpo.write(control_data)
            if eigthresh > 0:
                fpo.write(singular_value_decomposition)
            fpo.write(lsqr)
            fpo.write(parameter_groups)
            fpo.write("\n".join(parameter_data))

            fpo.write(observation_groups)

            fpo.write(observation_data)

            if model_command_line is None:
                raise ValueError(
                    warning(
                        """
                        The "model_command_line" keyword is required.
                        """
                    )
                )
            model_command_line = f"""
* model command line
{model_command_line}
"""
            fpo.write(model_command_line)

    def get_blocks(self, seq):
        """Return blocks of lines between "START ..." lines.

        The block below from a tsproc file::

            START GET_SERIES_WDM
             CONTEXT context_1
             NEW_SERIES_NAME IN02329500 IN02322500
             FILE data_test.wdm
             DSN 1 2
             FILTER -999
            END GET_SERIES_WDM

        Should yield::

           [
            [
              ['START', 'GET_SERIES_WDM'],
              ['CONTEXT', 'context_1'],
              ['NEW_SERIES_NAME', 'IN02329500', 'IN02322500'],
              ['FILE', 'data_test.wdm'],
              ['DSN', '1'],
              ['FILTER', '-999'],
              ['END', 'GET_SERIES_WDM']
            ],
           ]

        All arguments and keyword names are lower-cased internally.  This allows
        case insensitivity in the keyword value.
        """
        exceedance_pattern = re.compile(
            r"(^start  *|^end  *|^ *)exceedence", re.IGNORECASE
        )
        new_table_pattern = re.compile(r"^ *(new_table_name)", re.IGNORECASE)
        inblock = False
        data = []
        for line, index in seq:
            # Can't use 'tsutils.make_list(nline, sep=" ")' because need to have
            # number labels that are strings rather than integers.
            line = re.sub(exceedance_pattern, r"\1EXCEEDANCE", line)
            line = re.sub(new_table_pattern, r"NEW_G_TABLE_NAME", line)
            words = line.split()
            keyword = words[0].lower()

            # Test for "START ..." at the beginning of the line, start collecting
            # lines and yield data and line index when reaching the next "END ...".
            #
            # Collect the line index of the START keyword in order to report
            # location of any errors.
            if keyword == "start":
                inblock = True
                lindex = index + 1

            if inblock is True:
                if keyword == "start":
                    block_name = words[1].lower()
                data.append([keyword] + words[1:])

            if keyword == "end":
                if block_name != words[1].lower():
                    raise ValueError(
                        tsutils.error_wrapper(
                            f"""
                            The block name "{words[1]}" in the END line at line {index+1} does
                            not match the block name "{block_name}" in the START line.
                            """
                        )
                    )
                inblock = False
                yield data, lindex
                data = []

    def run(self, infile, running_context=None):
        """Parse a tsproc file."""
        _not_rollable_duplicate_keywords = {
            "exceedance_time",
            "flow_duration",
            "get_mul_series_gsflow_gage",
            "get_mul_series_plotgen",
            "get_mul_series_ssf",
            "get_mul_series_statvar",
            "hydrologic_indices",
            "list_output",
            "plot",
            "write_pest_files",
        }
        _not_rollable_multiple_entries = {
            "hydrologic_indices",
            "series_equation",
        }

        blocks = []
        lnumbers = []
        for index, (group, lnumber) in enumerate(
            self.get_blocks(self._read_file(infile))
        ):
            # Unroll the block.

            # First find the maximum number of words from each line in the
            # group and store in "maxl".
            maxl = 2
            rollable = True
            duplicates = False
            wpf = False
            for line in group:
                if line[1].lower() == "settings":
                    break
                if line[0].lower() == "end":
                    continue
                if line[1].lower() in _not_rollable_duplicate_keywords.union(
                    _not_rollable_multiple_entries
                ):
                    rollable = False
                if line[1].lower() in _not_rollable_duplicate_keywords:
                    duplicates = True

                # The following is to guarantee that WRITE_PEST_FILES is
                # preceded by a LIST_OUTPUT block.
                if line[1].lower() == "list_output":
                    prev_list_output_index = index
                if (
                    line[1].lower() == "write_pest_files"
                    and prev_list_output_index != index - 1
                ):
                    raise ValueError(
                        tsutils.error_wrapper(
                            """
                            The "WRITE_PEST_FILES" block must be
                            immediately preceded by a "LIST_OUTPUT" block.
                            The "LIST_OUTPUT" block should contain all the
                            simulated data to be used in the objective
                            function in the same order as listed in the
                            "WRITE_PEST_FILES" block.
                            """
                        )
                    )

                if line[1].lower() == "write_pest_files":
                    wpf = True

                if len(line) > maxl:
                    maxl = len(line)

            # Have to make sure that the "WRITE_PEST_FILES" block has *_weigth_min_max
            # keywords after every *_equation keyword.
            if wpf is True:
                ins_group = list(group)
                offset = 0
                for wpf_type in ["series", "s_table", "v_table", "e_table", "g_table"]:
                    for index in range(len(group)):
                        if group[index][0] == f"observation_{wpf_type}_name":
                            if group[index + 1][0] != f"model_{wpf_type}_name":
                                raise ValueError(
                                    tsutils.error_wrapper(
                                        f"""
                                        The "WRITE_PEST_FILES" block must have a
                                        "model_{wpf_type}_name" keyword immediately
                                        after the "observation_{wpf_type}_name"
                                        keyword.
                                        """
                                    )
                                )
                            if group[index + 2][0] != f"{wpf_type}_weights_equation":
                                raise ValueError(
                                    tsutils.error_wrapper(
                                        f"""
                                        The "WRITE_PEST_FILES" block must have a
                                        "{wpf_type}_weights_equation" keyword immediately
                                        after the "model_{wpf_type}_name" keyword.
                                        """
                                    )
                                )
                            if group[index + 3][0] != f"{wpf_type}_weights_min_max":
                                warning(
                                    """
                                        The "WRITE_PEST_FILES" block can have a
                                        optional "{wpf_type}_weights_min_max minval
                                        maxval" entry immediately after the
                                        "{wpf_type}_weights_equation" keyword.
                                        """
                                )
                                ins_group.insert(
                                    index + 3 + offset,
                                    [
                                        f"{wpf_type}_weights_min_max",
                                        "0",
                                        f"{sys.float_info.max}",
                                    ],
                                )
                                offset += 1
                group = ins_group

            # Use "maxl" loops to create new groups.
            if rollable is True:
                for unrolled in range(1, maxl):
                    ngroup = []
                    for line in group[:-1]:
                        # Take the "line[unrolled]" element if available,
                        # otherwise take the last element.
                        try:
                            ngroup.append([line[0]] + [line[unrolled]])
                        except IndexError:
                            ngroup.append([line[0]] + [line[-1]])
                    blocks.append(ngroup)
                    lnumbers.append(lnumber)
            else:
                ngroup = []
                ordered = OrderedDict()
                for line in group[:-1]:
                    if duplicates is True:
                        if line[0] in ordered.keys():
                            ordered[line[0]].append(" ".join(line[1:]))
                        else:
                            ordered[line[0]] = [" ".join(line[1:])]
                    else:
                        ngroup.append(line)
                ngroup.extend([key] + value for key, value in ordered.items())
                blocks.append(ngroup)
                lnumbers.append(lnumber)
                if maxl > 2 and ngroup[0][1].lower() not in (
                    "flow_duration",
                    "hydrologic_indices",
                    "series_equation",
                    "write_pest_files",
                ):
                    warning(
                        f"""
                        The block "{ngroup[0][1]}" starting at line {lnumber} is
                        not able to be unrolled because it allows duplicate
                        keywords. Warning that this block has multiple entries
                        for at least one of the keywords.  Only the first entry
                        will be used.
                        """
                    )

        if running_context is None:
            for block in blocks:
                if block[0] == ["start", "SETTINGS"]:
                    for blk in block[1:]:
                        if blk[0] == "context":
                            running_context = blk[1]
                            break
                    break

        runblocks = []
        nnumbers = []
        for block, lnum in zip(blocks, lnumbers):
            print("")
            context = False
            for line in block:
                if line[0] == "start":
                    block_name = line[1]
                if line[0] == "context":
                    context = True
                    if block_name == "SETTINGS":
                        print(f"# RUNNING SETTINGS block @ line {lnum}.")
                    elif line[1] == "all" or line[1] == running_context:
                        print(
                            f"# RUNNING following block @ line {lnum} because CONTEXT '{line[1]}' matches running CONTEXT."
                        )
                        runblocks.append(block)
                        nnumbers.append(lnum)
                    else:
                        print(
                            f"# SKIPPING following block @ line {lnum} because CONTEXT '{line[1]}' doesn't match running CONTEXT."
                        )
                    break
            if context is False:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The block "{block_name}" at line {lnum} does not have a
                        "context" keyword.
                        """
                    )
                )
            for line in block:
                if line[0] == "start":
                    block_name = line[1]
                    print(f"START {block_name}")
                else:
                    varl = " ".join(line[1:])
                    if varl.strip():
                        print(f"  {line[0].upper()} {varl}")
            print(f"END {block_name}")

        # Run the blocks.
        for block, lnum in zip(runblocks, nnumbers):
            keys = [i[0] for i in block if i[0] != "start"]
            self.line_number = lnum
            self.block_name = [i[1] for i in block if i[0] == "start"][0].upper()
            if self.block_name in deprecated:
                warning(
                    f"""
                    WARNING: The block "{self.block_name}" @ line number {lnum}
                    is deprecated within tsblender. {deprecated[self.block_name]}
                    """
                )
            args = [
                i.lower()
                for i in self.funcs[self.block_name]["args"]
                if i.lower() != "context"
            ]
            kwds = {
                key.lower(): val
                for key, val in self.funcs[self.block_name]["kwds"].items()
            }
            if any(elem not in keys for elem in args):
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        All parameters in "{args}" are required for
                        "{self.block_name}" at line {self.line_number}. You gave
                        "{keys}".
                        """
                    )
                )
            if all(item in args + list(kwds.keys()) for item in keys):
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The available parameters for "{self.block_name}" at line
                        "{self.line_number}" are "{args + list(kwds.keys())}" but
                        you gave "{set(args + list(kwds.keys()))-set(keys)}"
                        """
                    )
                )
            if self.block_name == "settings":
                continue
            parameters = {
                allv[0]: allv[1] if len(allv) == 2 else allv[1:] for allv in block
            }
            kwds.update(parameters)
            parameters = {key.lower(): val for key, val in kwds.items() if val}
            del parameters["start"]
            del parameters["context"]

            if os.path.exists("debug_tsblender"):
                print(
                    f"\nPROCESSING: {self.block_name} @ line number {self.line_number} with arguments {parameters}"
                )

            # Call the function with the args and kwds collected into the
            # parameters dictionary.
            self.funcs[self.block_name]["f"](**parameters)

        if os.path.exists("debug_tsblender"):
            print("\nTIME SERIES")
            print(self.series)
            print("\nS_TABLE")
            print(self.s_table)
            print("\nG_TABLE")
            print(self.g_table)
            print("\nE_TABLE")
            print(self.e_table)
            print("\nE_TABLE_TOT")
            print(self.e_table_tot)
            print("\nV_TABLE")
            print(self.v_table)
            print("\nC_TABLE")
            print(self.c_table)
            print(self.e_table_metadata)


@cltoolbox.command()
def run(infile, running_context: Optional[str] = None):
    data = Tables()
    data.run(infile, running_context)


def main():
    """Main function for command line."""
    if not os.path.exists("debug_tsblender"):
        sys.tracebacklimit = 0
    if os.path.exists("profile_tsblender"):
        import functiontrace

        functiontrace.trace()
    cltoolbox.main()


if __name__ == "__main__":
    """Set debug and run cltoolbox.main function."""
    main()
