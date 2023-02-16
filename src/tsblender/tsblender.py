"""Collection of functions for the manipulation of time series."""

import datetime
import os.path
import sys
import warnings
from collections import OrderedDict
from contextlib import suppress
from typing import Literal, Optional, Union

import cltoolbox
import numpy as np
import pandas as pd
import pyemu
from dateutil.parser import parse
from hydrotoolbox import hydrotoolbox
from hydrotoolbox.hydrotoolbox import baseflow_sep
from pydantic import validate_arguments
from scipy import signal
from toolbox_utils import tsutils
from toolbox_utils.readers.hbn import hbn_extract as _get_series_hbn
from toolbox_utils.readers.plotgen import plotgen_extract as _get_series_pgen
from toolbox_utils.readers.wdm import wdm_extract as _get_series_wdm
from tstoolbox.functions.gof import gof

warnings.filterwarnings("ignore")


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
    words = text.split(":")
    lets = words[0][:2]
    nums = words[0][2:]
    if len(nums) == 1:
        nums = "0" + nums
    return lets + nums


class Tables:
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

        self.funcs = {
            "SETTINGS": {
                "args": ["context", "date_format"],
                "kwds": {},
                "f": self.settings,
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
                    "SERIES_NAME": None,
                    "SERIES_FORMAT": "long",
                    "C_TABLE_NAME": None,
                    "S_TABLE_NAME": None,
                    "V_TABLE_NAME": None,
                    "E_TABLE_NAME": None,
                    "G_TABLE_NAME": None,
                },
                "f": self.list_output,
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
                "f": self.series_statistic,
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
                    "OBSERVATION_SERIES_NAME": None,
                    "MODEL_SERIES_NAME": None,
                    "SERIES_WEIGHTS_EQUATION": None,
                    "SERIES_WEIGHTS_MIN_MAX": None,
                    "OBSERVATION_S_TABLE_NAME": None,
                    "MODEL_S_TABLE_NAME": None,
                    "S_TABLE_WEIGHTS_EQUATION": None,
                    "S_TABLE_WEIGHTS_MIN_MAX": None,
                    "OBSERVATION_V_TABLE_NAME": None,
                    "MODEL_V_TABLE_NAME": None,
                    "V_TABLE_WEIGHTS_EQUATION": None,
                    "V_TABLE_WEIGHTS_MIN_MAX": None,
                    "OBSERVATION_E_TABLE_NAME": None,
                    "MODEL_E_TABLE_NAME": None,
                    "E_TABLE_WEIGHTS_EQUATION": None,
                    "E_TABLE_WEIGHTS_MIN_MAX": None,
                    "OBSERVATION_G_TABLE_NAME": None,
                    "MODEL_G_TABLE_NAME": None,
                    "G_TABLE_WEIGHTS_EQUATION": None,
                    "G_TABLE_WEIGHTS_MIN_MAX": None,
                    "AUTOMATIC_USER_INTERVENTION": "no",
                    "TRUNCATED_SVD": 2.0e-7,
                    "MODEL_COMMAND_LINE": None,
                },
                "f": self.write_pest_files,
            },
        }

    def _get_series(self, series_name: str):
        series_name = series_name.upper()
        whichdf = self.current_series[series_name]
        return self.series[whichdf][series_name]

    def _get_v_table(self, v_table_name: str):
        return self.v_table[v_table_name].dropna()

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

    def _normalize_dates(self, date, time="00:00:00"):
        if date is None:
            return None
        if time is None:
            time = "00:00:00"
        hours, minutes, seconds = time.split(":")
        delta = pd.Timedelta(days=0)
        if int(hours) == 24:
            hours = 0
            delta = pd.Timedelta(days=1)
        return (
            parse(f"{date} {int(hours):02}:{int(minutes):02}:{int(seconds):02}") + delta
        ).isoformat()

    def _normalize_bools(self, torf):
        if torf is True or torf is False:
            return torf
        if isinstance(torf, str):
            if torf.lower() in ("y", "yes"):
                return True
            if torf.lower() in ("n", "no"):
                return False
        if torf:
            return True
        return False

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

    def _prepare_series(
        self,
        series_name,
        log=False,
        power=1,
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

        test = [log]
        test.append(any(minmaxmeans.keys()))
        self._there_can_be_only_one(
            test, "'LOG' or any of the following: 'MAXMEAN_n', 'MINMEAN_n'"
        )

        test = [powertest]
        test.append(any(minmaxmeans.keys()))
        self._there_can_be_only_one(
            test, "'POWER' or any of the following: 'MAXMEAN_n', 'MINMEAN_n'"
        )
        series = self._get_series(series_name)
        series = series.asfreq(series.index.freqstr)

        if log:
            series = np.log10(series)

        if powertest:
            series = np.power(series, power)

        series = tsutils.common_kwds(
            series,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )

        series = series.iloc[:, 0]
        return series

    @validate_arguments
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

    @validate_arguments
    def erase_entity(
        self,
        series_name: Optional[str] = None,
        v_table_name: Optional[str] = None,
        c_table_name: Optional[str] = None,
        s_table_name: Optional[str] = None,
        e_table_name: Optional[str] = None,
        g_table_name: Optional[str] = None,
    ):
        if series_name is not None:
            series = series_name.upper()
            self.series[self.current_series[series]] = self.series[
                self.current_series[series]
            ].drop(series, axis="columns")
            del self.current_series[series]
            del self.series_dates[series]

        if v_table_name is not None:
            self.v_table = self.v_table.drop(v_table_name.upper(), axis="columns")
            del self.v_table_metadata[v_table_name.upper()]

        if c_table_name is not None:
            self.c_table = self.c_table.drop(c_table_name.upper(), axis="columns")
            del self.c_table_metadata[c_table_name.upper()]

        if s_table_name is not None:
            self.s_table = self.s_table.drop(s_table_name.upper(), axis="columns")
            del self.s_table_metadata[s_table_name.upper()]

        if e_table_name is not None:
            self.e_table = self.e_table.drop(e_table_name.upper(), axis="columns")
            self.e_table_tot = self.e_table_tot.drop(
                e_table_name.upper(), axis="columns"
            )
            del self.e_table_metadata[e_table_name.upper()]

        if g_table_name is not None:
            self.g_table = self.g_table.drop(g_table_name.upper(), axis="columns")
            del self.g_table_metadata[g_table_name.upper()]

    @validate_arguments
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
        series = self._get_series(series_name)

        year = datetime.timedelta(days=365, hours=6, minutes=9, seconds=9)
        punits = {
            "year": year,
            "month": year / 12,
            "day": datetime.timedelta(days=1),
            "hour": datetime.timedelta(hours=1),
            "min": datetime.timedelta(minutes=1),
            "sec": datetime.timedelta(seconds=1),
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
        if under_over == "over":
            tot_threshold = series.min()
        else:
            tot_threshold = series.max()
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
        e_table_tot.columns = [new_e_table_name.upper()]
        self._join(new_e_table_name, e_table_tot=e_table_tot)

        # This is here only to be able to include the time units in the
        # LIST_OUTPUT.
        self.e_table_metadata[new_e_table_name.upper()] = {
            "exceedance_time_units": exceedance_time_units,
            "source": series_name,
            "under_over": under_over,
        }

    @validate_arguments
    def flow_duration(
        self,
        series_name: str,
        new_g_table_name: str = None,
        new_table_name: str = None,
        exceedance_probabilities=(99.5, 99, 98, 95, 90, 75, 50, 25, 10, 5, 2, 1, 0.5),
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        self._there_can_be_only_one(
            [new_g_table_name, new_table_name], '"new_g_table_name" or "new_table_name"'
        )

        new_g_table_name = new_g_table_name or new_table_name

        series = self._get_series(series_name)
        series = tsutils.common_kwds(
            series,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
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

    @validate_arguments
    def get_series_csv(
        self,
        file: str,
        new_series_name: str,
        usecol: Union[int, str] = None,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        with suppress(ValueError):
            usecol = int(usecol)
        ts = tsutils.common_kwds(
            f"{file},{usecol}",
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        self._join(new_series_name, series=ts)

    @validate_arguments
    def get_series_gsflow_gage(
        self,
        file: str,
        data_type: str,
        new_series_name: str,
        model_reference_date: str,
        model_reference_time: str,
        time_units_per_day: int = 1,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        if isinstance(data_type, str):
            data_type = [data_type]
        if isinstance(new_series_name, str):
            new_series_name = [new_series_name]
        with open(file, encoding="ascii") as f:
            _ = f.readline()
            headers = f.readline()
        headers = headers.replace('"', "").split()
        headers = headers[1:]
        headers = [i.upper() for i in headers]
        ts = pd.read_csv(
            file,
            skiprows=2,
            sep=r"\s+",
            quoting=3,
            header=None,
            names=headers,
            index_col=0,
            engine="c",
        )

        unit = pd.Timedelta(1, "D") / time_units_per_day

        ts.index = (
            unit * ts.index
            + pd.to_datetime(model_reference_date + " " + model_reference_time)
            - unit
        )

        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )

        for dt, nsn in zip(data_type, new_series_name):
            try:
                nts = ts[dt.upper()]
            except KeyError as exc:
                raise KeyError(
                    tsutils.error_wrapper(
                        f"""
                        The time-series with variable name "{dt.upper()}" is
                        not available in file "{file}". The available
                        time-series are {ts.columns}.
                        """
                    )
                ) from exc
            self.series_dates[nsn.upper()] = [nts.index[0], nts.index[-1]]
            self._join(nsn.upper(), series=nts)

    @validate_arguments
    def get_series_statvar(
        self,
        file: str,
        variable_name,
        location_id,
        new_series_name,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        # Of the repeated keywords, make sure if there is a single item that it
        # is a list.
        if isinstance(variable_name, str):
            variable_name = [variable_name]
        if isinstance(location_id, (int, str)):
            location_id = [location_id]
        if isinstance(new_series_name, str):
            new_series_name = [new_series_name]

        # Need this to calculate "skiprows" in pd.read_csv and "headers" to
        # later rename the columns to.
        with open(file, encoding="ascii") as f:
            num_series = int(f.readline().strip())
            collect = []
            for ns in range(num_series):
                unique_id = f.readline().strip().split()
                collect.append(unique_id)
        headers = ["_".join(i) for i in collect]

        # Leave setting the index columns and headers to later since want
        # pd.read_csv to parse the dates from 6 columns which creates a new
        # column and messes with the order of the columns.
        ts = pd.read_csv(
            file,
            skiprows=num_series + 1,
            header=None,
            sep=r"\s+",
            parse_dates=[[1, 2, 3, 4, 5, 6]],
            date_parser=lambda x: pd.to_datetime(x, format="%Y %m %d %H %M %S"),
            engine="c",
        )
        ts = ts.set_index("1_2_3_4_5_6")
        ts = ts.drop(columns=[0])
        ts.index.name = "Datetime"
        ts.columns = headers

        # Use tsutils.common_kwds to subset the time period.
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )

        # Create a new DataFrame for each variable_name, location_id, and
        # new_series_name and join to the correct global DataFrame.
        # Update series metadata in self.series_dates.
        for vn, lid, nsn in zip(variable_name, location_id, new_series_name):
            try:
                nts = ts[f"{vn}_{lid}"]
            except KeyError as exc:
                raise KeyError(
                    tsutils.error_wrapper(
                        f"""
                        The time-series with variable name "{vn}" and location
                        ID "{lid}" forms the column name "{vn}_{lid}" and is
                        not available in file "{file}". The available
                        time-series are {ts.columns}.
                        """
                    )
                ) from exc
            self._join(nsn.upper(), series=nts)
            self.series_dates[nsn.upper()] = [nts.index[0], nts.index[-1]]

    @validate_arguments
    def get_series_hspfbin(
        self,
        file: str,
        new_series_name: str,
        interval: Literal["yearly", "monthly", "daily", "bivl"],
        operationtype: Literal["PERLND", "IMPLND", "RCHRES", "BMPRAC"],
        id: int,
        variable: str,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        ts = _get_series_hbn(
            file,
            interval,
            f"{operationtype},{id},,{variable}",
        )
        if len(ts.columns) == 0:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    No data found in the file when processing the block
                    {self.block_name} at line number {self.line_number}.
                    """
                )
            )
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        self._join(new_series_name, series=ts)

    @validate_arguments
    def get_series_plotgen(
        self,
        file: str,
        label,
        new_series_name: str,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        if isinstance(label, str):
            label = [label]
        if isinstance(new_series_name, str):
            new_series_name = [new_series_name]
        ts = _get_series_pgen(file)
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        for lb, nsn in zip(label, new_series_name):
            try:
                nts = ts[lb]
            except KeyError as exc:
                raise KeyError(
                    tsutils.error_wrapper(
                        f"""
                        The time-series "{lb}" is not available in file
                        "{file}". The available time-series are {ts.columns}.
                        """
                    )
                ) from exc
            self.series_dates[nsn.upper()] = [nts.index[0], nts.index[-1]]
            self._join(nsn.upper(), series=nts)

    @validate_arguments
    def get_series_ssf(
        self,
        file: str,
        site,
        new_series_name,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
    ):
        if isinstance(site, str):
            site = [site]
        if isinstance(new_series_name, str):
            new_series_name = [new_series_name]
        ts = pd.read_csv(
            file,
            header=None,
            parse_dates=[[1, 2]],
            index_col=[0],
            sep=r"\s+",
            dtype={0: str},
            engine="c",
        )
        try:
            ts = ts.pivot_table(
                index=ts.index,
                values=ts.columns.drop(ts.columns[0]),
                columns=ts.columns[0],
                aggfunc="first",
            )
        except ValueError as exc:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    Duplicate index (time stamp and '{ts.columns[0]}') were
                    found. Found these duplicate indices:
                    {ts.index.get_duplicates()}
                    """
                )
            ) from exc
        ts.index.name = "Datetime"
        ts.columns = [i[1] for i in ts.columns]

        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        ts.index = ts.index.to_period(ts.index[1] - ts.index[0]).to_timestamp()
        for st, nsn in zip(site, new_series_name):
            try:
                nts = ts[st]
            except KeyError as exc:
                raise KeyError(
                    tsutils.error_wrapper(
                        f"""
                        The time-series "{st}" is not available in file
                        "{file}". The available time-series are {ts.columns}.
                        """
                    )
                ) from exc
            self.series_dates[nsn.upper()] = [nts.index[0], nts.index[-1]]
            self._join(nsn.upper(), series=nts)

    @validate_arguments
    def get_series_wdm(
        self,
        file: str,
        new_series_name: str,
        dsn: int,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
        def_time="00:00:00",
        filter: Optional[int] = None,
    ):
        ts = pd.DataFrame(_get_series_wdm(file, dsn))
        hh, mm, ss = def_time.split(":")
        if int(hh) == 24:
            delta = pd.Timedelta(days=1, minutes=int(mm), seconds=int(ss))
        else:
            delta = pd.Timedelta(hours=int(hh), minutes=int(mm), seconds=int(ss))
        ts.index = ts.index + delta
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        self._join(new_series_name, series=ts)

    @validate_arguments
    def get_series_xlsx(
        self,
        new_series_name: str,
        file: str,
        sheet: Union[int, str] = 1,
        column: Union[int, str] = 1,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        ts = tsutils.common_kwds(
            f"{file},{sheet}",
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        try:
            ts = ts.iloc[:, int(column) - 1]
        except ValueError:
            ts = ts.loc[:, column]
        self._join(new_series_name, series=ts)

    @validate_arguments
    def hydrologic_indices(
        self,
        series_name: str,
        new_g_table_name: str,
        drainage_area: str = 1,
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
            if not isinstance(value, list):
                value = [value]
            ind.extend([f"{key}{i}" for i in value])
        if stream_classification is not None:
            ind.extend(
                [f"{sc}" for sc in tsutils.make_list(stream_classification, sep=" ")]
            )

        if flow_component is not None:
            ind.extend([f"{fc}" for fc in tsutils.make_list(flow_component, sep=" ")])

        series = self._get_series(series_name)
        series = tsutils.common_kwds(
            series,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
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

    @validate_arguments
    def hydro_events(
        self,
        series_name: str,
        new_series_name: str,
        rise_lag,
        fall_lag,
        window: int = 1,
        min_peak: int = 0,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        ts = pd.DataFrame(self._get_series(series_name))
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        ts = hydrotoolbox.storm_events(
            rise_lag, fall_lag, input_ts=ts, window=window, min_peak=min_peak
        )

        self._join(new_series_name, series=ts)

    @validate_arguments
    def list_output(
        self,
        file,
        series_name=None,
        series_format="long",
        s_table_name=None,
        c_table_name=None,
        v_table_name=None,
        e_table_name=None,
        g_table_name=None,
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
        if isinstance(series_name, str):
            series_name = [series_name]
        elif series_name is None:
            series_name = []
        if isinstance(s_table_name, str):
            s_table_name = [s_table_name]
        elif s_table_name is None:
            s_table_name = []
        if isinstance(c_table_name, str):
            c_table_name = [c_table_name]
        elif c_table_name is None:
            c_table_name = []
        if isinstance(v_table_name, str):
            v_table_name = [v_table_name]
        elif v_table_name is None:
            v_table_name = []
        if isinstance(e_table_name, str):
            e_table_name = [e_table_name]
        elif e_table_name is None:
            e_table_name = []
        if isinstance(g_table_name, str):
            g_table_name = [g_table_name]
        elif g_table_name is None:
            g_table_name = []

        self.ofile = _ins_file or file

        # Time series first
        with open(self.ofile, "w", encoding="utf-8") as fp:
            outp = "pif $\n1l\n" if _ins_file else ""
            fp.write(outp)
            for sern in series_name:
                outp = "31\n" if _ins_file else f'\n TIME_SERIES "{sern}" ---->\n'
                fp.write(outp)
                series = self._get_series(sern.upper())
                start, end = self.series_dates[sern.upper()]
                for rowno, (index, value) in enumerate(
                    series.loc[start:end].dropna().iteritems()
                ):
                    if series_format == "long":
                        date = index.strftime(self.date_format)
                        time = index.strftime("%H:%M:%S")
                        outp = (
                            f"l1   [{sern}{rowno+1}]\n"
                            if _ins_file
                            else f" {sern}{date:>{28-len(sern)}}   {time} {value:<13.13g}\n"
                        )
                    elif series_format == "short":
                        outp = (
                            "1l    [{sern}{rowno+1}]\n" if _ins_file else f" {value}\n"
                        )
                    fp.write(outp.rstrip() + "\n")
            outp = "11\n" if _ins_file else "\n"
            fp.write(outp)

            for s_tab in s_table_name:
                s_TAB = s_tab.upper()
                st = self.s_table[s_TAB].dropna()
                source_name = self.s_table_metadata[s_TAB]["source_name"]
                start_date = self.s_table_metadata[s_TAB]["start_date"].strftime(
                    "%Y-%m-%d"
                )
                end_date = self.s_table_metadata[s_TAB]["end_date"].strftime("%Y-%m-%d")
                log_transformed = self.s_table_metadata[s_TAB]["log_transformed"]
                exponent = self.s_table_metadata[s_TAB]["exponent"]
                outp = (
                    "l8\n"
                    if _ins_file
                    else f"""
 S_TABLE "{s_tab}" ---->
     Series for which data calculated:               {source_name}
     Starting date for data accumulation:            {start_date}
     Ending date for data accumulation               {end_date}
     Logarithmic transformation of series?           {log_transformed}
     Exponent in power transformation:               {exponent}
"""
                )
                fp.write(outp)
                for index, value in st.iteritems():
                    outp = (
                        "1l {s_tab}  \n"
                        if _ins_file
                        else f"     {s_tab}{index:>{28-len(s_tab)}}   {index.strftime('%H:%M:%S')} {value:<13.13g}\n"
                    )
                    fp.write(outp)

            for c_tab in c_table_name:
                c_TAB = c_tab.upper()
                stats = self.c_table[c_TAB].dropna()
                obs_name = self.c_table_metadata[c_TAB]["obs_name"]
                sim_name = self.c_table_metadata[c_TAB]["sim_name"]
                start_date = self.c_table_metadata[c_TAB]["start_date"].strftime(
                    "%Y-%m-%d"
                )
                end_date = self.c_table_metadata[c_TAB]["end_date"].strftime("%Y-%m-%d")
                outp = (
                    "FIXME"
                    if _ins_file
                    else f"""
 C_TABLE "{c_tab}" ---->
     Observation time series name:                    {obs_name}
     Simulation time series name:                     {sim_name}
     Beginning time of series comparison:             {start_date}
     Finishing time of series comparison:             {end_date}
"""
                )
                fp.write(outp)
                for index, value in stats.iteritems():
                    outp = (
                        "FIXME"
                        if _ins_file
                        else f"     {c_tab}{index:>{28-len(c_tab)}}   {index.strftime('%H:%M:%S')} {value:<13.13g}\n"
                    )
                    fp.write(outp)

            for v_tab in v_table_name:
                outp = (
                    "l4"
                    if _ins_file
                    else f"""
 V_TABLE "{v_tab}" ---->
     Volumes calculated from series "{self.v_table_metadata[v_tab.upper()]["source_name"]}" are as follows:-
"""
                )
                fp.write(outp)
                v_table = self._get_v_table(v_tab.upper())
                for index, value in v_table.dropna().iteritems():
                    outp = (
                        "FIXME"
                        if _ins_file
                        else f"    From {index[0].strftime(self.date_format)} {index[0].strftime('%H:%M:%S')} to {index[1].strftime(self.date_format)} {index[1].strftime('%H:%M:%S')}  volume = {value:0<13.13g}\n"
                    )
                    fp.write(outp)

            for e_tab in e_table_name:
                e_TAB = self.e_table[e_tab.upper()]
                et_tot = self.e_table_tot[e_tab.upper()]
                units = self.e_table_metadata[e_tab.upper()]["exceedance_time_units"]
                direction = {"over": "above", "under": "below"}[
                    self.e_table_metadata[e_tab.upper()]["under_over"]
                ]
                outp = (
                    "4l"
                    if _ins_file
                    else f"""
 E_TABLE "{e_tab}" ---->
   Flow           Time delay ({units})    Time {direction} ({units})   Fraction of time {direction} threshold
"""
                )
                fp.write(outp)
                for index, value in e_TAB.dropna().iteritems():
                    outp = (
                        "FIXME"
                        if _ins_file
                        else f"   {index[0]:<7g}          {index[1]:<7.4f}{value:>25.12g}{et_tot[index]:>25.10g}\n"
                    )
                    fp.write(outp)

            for g_tab in g_table_name:
                src = self.g_table_metadata[g_tab.upper()]["source"]
                kind = self.g_table_metadata[g_tab.upper()]["kind"]
                start_date = self.g_table_metadata[g_tab.upper()]["start_date"]
                end_date = self.g_table_metadata[g_tab.upper()]["end_date"]

                if kind == "flow_duration":
                    #  G_TABLE "mduration" ---->
                    #    Flow-duration curve for series "mflow" (11/08/8672 to 11/07/8693)                     Value
                    #    99.50% of flows exceed:                                                         4.912684
                    outp = (
                        "FIXME"
                        if _ins_file
                        else f"""
 G_TABLE "{g_tab}" ---->
   Flow-duration curve for series "{src}" ({start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")})   Value
"""
                    )
                    fp.write(outp)
                    g_table = self.g_table[g_tab.upper()].dropna()
                    g_table = g_table.sort_index(ascending=False)
                    for index, value in g_table.dropna().iteritems():
                        outp = (
                            "FIXME"
                            if _ins_file
                            else f"   {index*100:5.02f}% of flows exceed:{value:>60.15g}\n"
                        )
                        fp.write(outp)
                elif kind == "hydrologic_indices":
                    #  G_TABLE "mduration_p" ---->
                    #    Hydrologic Index and description (Olden and Poff, 2003)                               Value
                    #    MA16: Mean monthly flow May-Aug:                                                      4.912684
                    outp = (
                        "FIXME"
                        if _ins_file
                        else f"""
 G_TABLE "{g_tab}" ---->
   Hydrologic Index:description (Olden & Poff, 2003) {src} {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")} {"Value":>{32-len(src)}}
"""
                    )
                    fp.write(outp)

                    g_table = self.g_table[g_tab.upper()].dropna()
                    sindex = sorted(g_table.index, key=natural_keys)
                    g_table = g_table.loc[sindex]

                    for index, value in g_table.iteritems():
                        outp = (
                            "FIXME"
                            if _ins_file
                            else f"   {index}:{value:>{107-len(index)}.15g}\n"
                        )
                        fp.write(outp)

    @validate_arguments
    def new_series_uniform(
        self,
        new_series_name: str,
        new_series_value: float,
        time_interval: int,
        time_unit: str,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
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
            start=self._normalize_dates(date_1, time_1),
            end=self._normalize_dates(date_2, time_2),
            freq=ptunit,
        )
        ndf = pd.DataFrame(data=[new_series_value] * len(ndr), index=ndr)
        self._join(new_series_name, series=ndf)

    @validate_arguments
    def new_time_base(
        self, series_name: str, new_series_name: str, tb_series_name: str
    ):
        tb_series = pd.DataFrame(self._get_series(tb_series_name)).dropna()
        series = pd.DataFrame(self._get_series(series_name))
        nseries = tb_series.join(series, how="outer").iloc[:, 1].astype(float)
        nseries = nseries.interpolate(method="time", limit_direction="both").loc[
            tb_series.index
        ]
        self._join(new_series_name, series=nseries)

    @validate_arguments
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
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
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

    @validate_arguments
    def reduce_time_span(
        self,
        series_name: str,
        new_series_name: str,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        series = self._get_series(series_name)
        series = tsutils.common_kwds(
            series,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_base_level(
        self,
        series_name: str,
        substitute,
        new_series_name: str,
        base_level_series_name: str,
        base_level_date: str,
        base_level_time: str,
        negate: bool = False,
    ):
        series = self._get_series(series_name)
        base_level = self._get_series(base_level_series_name)
        base = base_level[pd.to_datetime(f"{base_level_date} {base_level_time}")]
        series = series - base
        if self._normalize_bools(negate):
            series = -series
        if self._normalize_bools(substitute):
            new_series_name = series_name
            self.erase_entity(series_name=series_name)
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_clean(
        self,
        series_name: str,
        new_series_name: str,
        substitute_value: Union[float, Literal["delete"]],
        lower_erase_boundary: Optional[float] = None,
        upper_erase_boundary: Optional[float] = None,
    ):
        if substitute_value == "delete":
            substitute_value = pd.NA
        series = self._get_series(series_name)
        if lower_erase_boundary is not None:
            series[series > lower_erase_boundary] = substitute_value
        if upper_erase_boundary is not None:
            series[series < upper_erase_boundary] = substitute_value
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_compare(
        self,
        series_name_sim: str,
        series_name_obs: str,
        new_c_table_name: str,
        bias=False,
        standard_error=False,
        relative_bias=False,
        relative_standard_error=False,
        nash_sutcliffe=False,
        coefficient_of_efficiency=False,
        index_of_agreement=False,
        volumetric_efficiency=False,
        exponent=2,
        series_name_base: str = "",
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        series_sim = self._get_series(series_name_sim)
        series_sim = tsutils.common_kwds(
            series_sim,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        series_obs = self._get_series(series_name_obs)
        series_obs = tsutils.common_kwds(
            series_obs,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        if series_name_base:
            series_base = self._get_series(series_name_base)
            series_base = tsutils.common_kwds(
                series_base,
                start_date=self._normalize_dates(date_1, time_1),
                end_date=self._normalize_dates(date_2, time_2),
            )

        c_table = [
            "Bias:",
            "Standard error:",
            "Relative bias:",
            "Relative standard error:",
            "Nash-Sutcliffe coefficient:",
            "Coefficient of efficiency:",
            "Index of agreement:",
            "Volumetric efficiency:",
        ]

        c_table = pd.DataFrame(data=[pd.NA] * len(c_table), index=c_table)

        stats = {}
        if self._normalize_bools(bias):
            stats["Bias:"] = gof(stats="me", sim_col=series_sim, obs_col=series_obs)[0][
                1
            ]
        if self._normalize_bools(standard_error):
            stats["Standard error:"] = gof(
                stats="rmse", sim_col=series_sim, obs_col=series_obs
            )[0][1]
        if self._normalize_bools(relative_bias):
            stats["Relative bias:"] = (
                gof(stats="me", sim_col=series_sim, obs_col=series_obs)[0][1]
                / series_obs.mean()
            )
        if self._normalize_bools(relative_standard_error):
            stats["Relative standard error:"] = gof(
                stats="nrmse_mean", sim_col=series_sim, obs_col=series_obs
            )[0][1]
        if self._normalize_bools(nash_sutcliffe):
            stats["Nash-Sutcliffe coefficient:"] = gof(
                stats="nse", sim_col=series_sim, obs_col=series_obs
            )[0][1]
        if self._normalize_bools(coefficient_of_efficiency):
            if exponent == 2:
                stats["Coefficient of efficiency:"] = gof(
                    stats="lm_index", sim_col=series_sim, obs_col=series_obs
                )[0][
                    1
                ]  # need to fix
            elif exponent == 1:
                stats["Coefficient of efficiency:"] = gof(
                    stats="lm_index", sim_col=series_sim, obs_col=series_obs
                )[0][1]
        if self._normalize_bools(index_of_agreement):
            if exponent == 2:
                stats["Index of agreement:"] = gof(
                    stats="d", sim_col=series_sim, obs_col=series_obs
                )[0][1]
            elif exponent == 1:
                stats["Index of agreement:"] = gof(
                    stats="d1", sim_col=series_sim, obs_col=series_obs
                )[0][1]
        if self._normalize_bools(volumetric_efficiency):
            stats["Volumetric efficiency:"] = gof(
                stats="ve", sim_col=series_sim, obs_col=series_obs
            )[0][1]

        nc_table = pd.DataFrame.from_dict(stats, orient="index")

        self._join(new_c_table_name, c_table=nc_table)
        self.c_table_metadata[new_c_table_name.upper()] = {
            "sim_name": series_name_sim,
            "obs_name": series_name_obs,
            "start_date": series_sim.index[0],
            "end_date": series_sim.index[-1],
        }

    @validate_arguments
    def series_difference(
        self,
        series_name: str,
        new_series_name: str,
    ):
        series = self._get_series(series_name)
        series = series.diff()
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_displace(
        self,
        series_name,
        new_series_name,
        lag_increment: int,
        fill_value,
    ):
        series = self._get_series(series_name)
        series = series.shift(lag_increment)
        series[-lag_increment:] = fill_value  # Verify what TSPROC does.
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_equation(
        self,
        new_series_name: str,
        equation,
    ):
        if isinstance(equation, list):
            equation = "".join(equation)
        equation = equation.strip().lower()
        keys = sorted(self.current_series.keys(), key=len, reverse=True)
        for i in keys:
            equation = equation.replace(i.lower(), f"self._get_series('{i.upper()}')")
        series = eval(equation)
        self._join(new_series_name, series=series)

    @validate_arguments
    def series_statistic(
        self,
        series_name: str,
        new_s_table_name: str,
        sum=False,
        mean=False,
        median=False,
        std_dev=False,
        maximum=False,
        minimum=False,
        range=False,
        log=False,
        power: float = 1,
        date_1: Optional[str] = None,
        time_1: Optional[str] = None,
        date_2: Optional[str] = None,
        time_2: Optional[str] = None,
        **minmaxmeans,
    ):
        log = self._normalize_bools(log)
        series = self._prepare_series(
            series_name,
            log=log,
            power=power,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
            **minmaxmeans,
        )

        rows = [
            "sum",
            "mean",
            "median",
            "std_dev",
            "maximum",
            "minimum",
            "range",
        ]
        minmaxmeans = {
            key: self._normalize_bools(value)
            for key, value in minmaxmeans.items()
            if value
        }
        for mmm in minmaxmeans:
            rows.append(mmm)
        s_table = pd.DataFrame([pd.NA] * len(rows), index=rows)

        if self._normalize_bools(sum):
            s_table.loc["sum", :] = series.sum()
        if self._normalize_bools(mean):
            s_table.loc["mean", :] = series.mean()
        if self._normalize_bools(median):
            s_table.loc["median", :] = series.median()
        if self._normalize_bools(std_dev):
            s_table.loc["std_dev", :] = series.std()
        if self._normalize_bools(maximum):
            s_table.loc["maximum", :] = series.max()
        if self._normalize_bools(minimum):
            s_table.loc["minimum", :] = series.min()
        if self._normalize_bools(range):
            s_table.loc["range", :] = series.max() - series.min()
        for key, value in minmaxmeans.items():
            if "minmean" not in key and "maxmean" not in key:
                raise ValueError(
                    tsutils.error_wrapper(
                        f"""
                        The key '{key}' is not valid. It must be either
                        "minmean" or "maxmean"
                        """
                    )
                )
            stat, interval = key.split("_")
            interval = int(interval)
            if value:
                grouped = series.groupby(
                    np.arange(len(series.index)) // interval
                ).mean()
                if "max" in stat:
                    s_table.loc[key, :] = grouped.max()
                if "min" in stat:
                    s_table.loc[key, :] = grouped.min()
        self._join(new_s_table_name, s_table=s_table)
        self.s_table_metadata[new_s_table_name] = {
            "source_name": series_name,
            "start_date": series[0],
            "end_date": series[-1],
            "log_transformed": log,
            "exponent": power,
        }

    def settings(self, date_format="%Y-%m-%d"):
        if date_format == "dd/mm/yyyy":
            date_format = "%d/%m/%Y"
        if date_format == "mm/dd/yyyy":
            date_format = "%m/%d/%Y"
        self.date_format = date_format

    @validate_arguments
    def v_table_to_series(
        self,
        new_series_name,
        v_table_name,
        time_abscissa,
    ):
        v_table = self._get_v_table(v_table_name)
        series = v_table  # FIX!
        self._join(new_series_name, series=series)

    @validate_arguments
    def volume_calculation(
        self,
        series_name,
        new_v_table_name,
        flow_time_units,
        date_file=None,
        automatic_dates=None,
        factor=1,
    ):
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
                for line in fpi.readlines():
                    words = line.strip().split()
                    start_end.append(
                        [
                            self._normalize_dates(words[0], words[1]),
                            self._normalize_dates(words[2], words[3]),
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

    @validate_arguments
    def usgs_hysep(
        self,
        series_name: str,
        new_series_name: str,
        hysep_type: str,
        time_interval: int,
        area: float = 1.0,
        date_1: str = None,
        time_1: str = None,
        date_2: str = None,
        time_2: str = None,
    ):
        ts = pd.DataFrame(self._get_series(series_name))
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
        )
        if time_interval is None:
            time_interval = 5
        if hysep_type.lower() == "fixed":
            ts = hydrotoolbox.baseflow_sep.usgs_hysep_fixed(ts, time_interval)
        elif hysep_type.lower() == "local":
            ts = hydrotoolbox.baseflow_sep.usgs_hysep_local(ts, time_interval)
        elif hysep_type.lower() == "sliding":
            ts = hydrotoolbox.baseflow_sep.usgs_hysep_slide(ts, time_interval)

        self._join(new_series_name, series=ts)

    @validate_arguments
    def write_pest_files(
        self,
        new_pest_control_file,
        new_instruction_file,
        series_name=None,
        template_file=None,
        model_input_file=None,
        parameter_data_file=None,
        parameter_group_file=None,
        observation_series_name=None,
        model_series_name=None,
        series_weights_equation=None,
        series_weights_min_max=None,
        observation_s_table_name=None,
        model_s_table_name=None,
        s_table_weights_equation=None,
        s_table_weights_min_max=None,
        observation_v_table_name=None,
        model_v_table_name=None,
        v_table_weights_equation=None,
        v_table_weights_min_max=None,
        observation_e_table_name=None,
        model_e_table_name=None,
        e_table_weights_equation=None,
        e_table_weights_min_max=None,
        observation_g_table_name=None,
        model_g_table_name=None,
        g_table_weights_equation=None,
        g_table_weights_min_max=None,
        automatic_user_intervention="no",
        truncated_svd=2.0e-7,
        model_command_line=None,
        **kwds,
    ):
        pst = pyemu.Pst.from_io_files(
            template_file, model_input_file, new_instruction_file, self.ofile
        )
        pst.write(new_pest_control_file)


def get_blocks(seq):
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
    inblock = False
    data = []
    for index, line in enumerate(seq):
        nline = line.strip()

        # Handle comment lines and partial comment lines.  Everything from
        # a "#" to the end of the line is a comment.
        with suppress(ValueError):
            nline = nline[: nline.index("#")].rstrip()

        # Handle blank lines.
        if not nline:
            continue

        # Can't use 'tsutils.make_list(nline, sep=" ")' because need to have
        # number labels that are strings rather than integers.
        words = nline.split()
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
                        The block name in the END line at line {index+1} does
                        not match the block name in the START line.
                        """
                    )
                )
            inblock = False
            yield data, lindex
            data = []


@cltoolbox.command()
def run(infile, running_context=None):
    """Parse a tsproc file."""
    blocks = []
    lnumbers = []
    with open(infile, encoding="utf-8") as fpi:
        for index, group_lnumber in enumerate(get_blocks(fpi)):
            group, lnumber = group_lnumber
            # Unroll the block.

            # First find the maximum number of words from each line in the
            # group and store in "maxl".
            maxl = 2
            rollable = True
            duplicates = False
            for line in group:
                if line[1].lower() == "settings":
                    break
                if line[0].lower() == "end":
                    continue
                if line[1].lower() in (
                    "get_mul_series_plotgen",
                    "get_mul_series_gsflow_gage",
                    "get_mul_series_ssf",
                    "get_mul_series_statvar",
                    "exceedance_time",
                    "hydrologic_indices",
                    "list_output",
                    "write_pest_files",
                    "flow_duration",
                    "series_equation",
                ):
                    rollable = False
                if line[1].lower() in (
                    "get_mul_series_plotgen",
                    "get_mul_series_gsflow_gage",
                    "get_mul_series_ssf",
                    "get_mul_series_statvar",
                    "exceedance_time",
                    "hydrologic_indices",
                    "list_output",
                    "write_pest_files",
                ):
                    duplicates = True

                # The following is to guarantee that WRITE_PEST_FILES is
                # preceded by a LIST_OUTPUT block.
                if line[1].lower() == "list_output":
                    prev_list_output_index = index
                if line[1].lower() == "write_pest_files":
                    if prev_list_output_index != index - 1:
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

                if len(line) > maxl:
                    maxl = len(line)

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
                            ordered[line[0]].append(line[1])
                        else:
                            ordered[line[0]] = [line[1]]
                    else:
                        ngroup.append(line)
                for key, value in ordered.items():
                    ngroup.append([key] + value)
                blocks.append(ngroup)
                lnumbers.append(lnumber)

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
        for line in block:
            if (line[0] == "context" and line[1] == running_context) or line[
                1
            ] == "all":
                runblocks.append(block)
                nnumbers.append(lnum)
                break
    data = Tables()

    for block, lnum in zip(runblocks, nnumbers):
        keys = [i[0] for i in block if i[0] != "start"]
        data.line_number = lnum
        data.block_name = [i[1] for i in block if i[0] == "start"][0].upper()
        if data.block_name in deprecated:
            warning(
                f"""
                WARNING: The block "{data.block_name}" is deprecated within
                tsblender. {deprecated[data.block_name]}"""
            )
        args = [
            i.lower()
            for i in data.funcs[data.block_name]["args"]
            if i.lower() != "context"
        ]
        kwds = {
            key.lower(): val for key, val in data.funcs[data.block_name]["kwds"].items()
        }
        if not all(elem in keys for elem in args):
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    All parameters in "{args}" are required for
                    "{data.block_name}" at line {data.line_number}. You gave
                    "{keys}".
                    """
                )
            )
        if all(item in args + list(kwds.keys()) for item in keys):
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
                    The available parameters for "{data.block_name}" at line
                    "{data.line_number}" are "{args + list(kwds.keys())}" but
                    you gave "{set(args + list(kwds.keys()))-set(keys)}"
                    """
                )
            )
        if data.block_name == "settings":
            continue
        parameters = {}
        for allv in block:
            if len(allv) == 2:
                parameters[allv[0]] = allv[1]
            else:
                parameters[allv[0]] = allv[1:]
        kwds.update(parameters)
        parameters = {key.lower(): val for key, val in kwds.items() if val is not None}
        del parameters["start"]
        del parameters["context"]
        if os.path.exists("debug_tsblender"):
            print(
                f"PROCESSING: {data.block_name} @ line number {data.line_number} with arguments {parameters}"
            )
        data.funcs[data.block_name]["f"](**parameters)
    if os.path.exists("debug_tsblender"):
        print("\nTIME SERIES")
        print(data.series)
        print("\nS_TABLE")
        print(data.s_table)
        print("\nG_TABLE")
        print(data.g_table)
        print("\nE_TABLE")
        print(data.e_table)
        print("\nE_TABLE_TOT")
        print(data.e_table_tot)
        print("\nV_TABLE")
        print(data.v_table)
        print("\nC_TABLE")
        print(data.c_table)
        print(data.e_table_metadata)


def main():
    """Set debug and run cltoolbox.main function."""
    if not os.path.exists("debug_tsblender"):
        sys.tracebacklimit = 0
    if os.path.exists("profile_tsblender"):
        import functiontrace

        functiontrace.trace()
    cltoolbox.main()


if __name__ == "__main__":
    main()
