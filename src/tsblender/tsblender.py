# -*- coding: utf-8 -*-
"""Collection of functions for the manipulation of time series."""


import os.path
import sys
import warnings
from typing import Literal, Optional, Union

import mando
import pandas as pd
from tstoolbox import tsutils

from .functions.get_series_wdm import get_wdm_data_set as get_series_wdm

warnings.filterwarnings("ignore")


@mando.command()
def about():
    """Display version number and system information."""
    tsutils.about(__name__)


class Tables:
    def __init__(self):
        self.series = {
            "S": pd.DataFrame(),
            "T": pd.DataFrame(),
            "H": pd.DataFrame(),
            "D": pd.DataFrame(),
            "M": pd.DataFrame(),
            "A-DEC": pd.DataFrame(),
        }
        self.v_table = {
            "S": pd.DataFrame(),
            "T": pd.DataFrame(),
            "H": pd.DataFrame(),
            "D": pd.DataFrame(),
            "M": pd.DataFrame(),
            "A-DEC": pd.DataFrame(),
        }
        self.c_table = pd.DataFrame()
        self.s_table = pd.DataFrame()
        self.e_table = pd.DataFrame()
        self.g_table = pd.DataFrame()
        self.current = {}

        self.funcs = {
            "SETTINGS": {
                "args": ["context", "date_format"],
                "kwds": {},
                "f": lambda x: x,
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
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "FLOW_DURATION": {
                "args": [
                    "CONTEXT" "SERIES_NAME",
                    "NEW_G_TABLE_NAME",
                ],
                "kwds": {
                    "EXCEEDENCE_PROBABILITIES": [
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
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "GET_SERIES_CSV": {
                "args": ["CONTEXT", "FILE", "SITE", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                    "USE_COLS": None,
                    "PARSE_DATES": None,
                    "INDEX_COLS": None,
                },
                "f": self.get_series_csv,
            },
            "GET_SERIES_HSPFBIN": {
                "args": ["CONTEXT", "FILE", "NEW_SERIES_NAME", "INTERVAL"],
                "kwds": {
                    "OPERATIONTYPE": None,
                    "ID": None,
                    "VARIABLE_GROUP": None,
                    "VARIABLE": None,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": self.get_series_hspfbin,
            },
            "GET_SERIES_PLOTGEN": {
                "args": ["CONTENT", "FILE", "LABEL", "NEW_SERIES_NAME"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "GET_SERIES_SWMMBIN": {
                "args": ["CONTENT"],
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
                "args": ["CONTENT"],
                "kwds": {
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "HYDROLOGIC_INDICES": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_G_TABLE_NAME"],
                "kwds": {
                    "USE_MEDIAN": False,
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
                },
                "f": self.hydrologic_indices,
            },
            "LIST_OUTPUT": {
                "args": ["CONTEXT", "FILE", "SERIES_FORMAT"],
                "kwds": {
                    "SERIES_NAME": None,
                    "C_TABLE_NAME": None,
                    "S_TABLE_NAME": None,
                    "V_TABLE_NAME": None,
                    "E_TABLE_NAME": None,
                    "G_TABLE_NAME": None,
                },
                "f": lambda x: x,
            },
            "NEW_SERIES_UNIFORM": {
                "args": [
                    "CONTEXT",
                    "NEW_SERIES_NAME",
                    "DATE_1",
                    "TIME_1",
                    "DATE_2",
                    "TIME_2",
                    "TIME_INTERVAL",
                    "TIME_UNIT",
                    "NEW_SERIES_NAME",
                ],
                "kwds": {},
                "f": lambda x: x,
            },
            "NEW_TIME_BASE": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME", "TB_SERIES_NAME"],
                "kwds": {},
                "f": lambda x: x,
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
                    "YEAR_TIME": "water_high",
                    "LOG": "no",
                    "POWER": 1,
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": lambda x: x,
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
                    "NEGATE": "no",
                },
                "f": lambda x: x,
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
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "SERIES_DIFFERENCE": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_SERIES_NAME"],
                "kwds": {},
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
            "SERIES_STATISTICS": {
                "args": ["CONTEXT", "SERIES_NAME", "NEW_S_TABLE_NAME"],
                "kwds": {
                    "SUM": "no",
                    "MEAN": "no",
                    "MEDIAN": "no",
                    "MINMEAN": "no",
                    "MAXMEAN": "no",
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
                "f": series_statistic,
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
                    "DATE_1": None,
                    "TIME_1": None,
                    "DATE_2": None,
                    "TIME_2": None,
                },
                "f": usgs_hysep,
            },
            "V_TABLE_TO_SERIES": {
                "args": ["CONTEXT", "NEW_SERIES_NAME", "V_TABLE_NAME", "TIME_ABSCISSA"],
                "kwds": {},
                "f": lambda x: x,
            },
            "VOLUME_CALCULATION": {
                "args": ["CONTEXT", "NEW_V_TABLE_NAME", "DATE_FILE", "FLOW_TIME_UNITS"],
                "kwds": {
                    "FACTOR": 1.0,
                },
                "f": lambda x: x,
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
                "f": lambda x: x,
            },
        }

    def _join(
        self,
        series=None,
        v_table=None,
        c_table=None,
        s_table=None,
        e_table=None,
        g_table=None,
    ):
        if series is not None:
            col = series.columns[0]
            if col in self.current:
                raise ValueError(f"{col} is already a named series")
            self.current[col] = series.index.freqstr
            self.series[series.index.freqstr] = self.series[series.index.freqstr].join(
                series, how="outer"
            )
        if v_table is not None:
            col = v_table.columns[0]
            if col in self.current:
                raise ValueError(f"{col} is already a named v_table")
            self.current[col] = v_table.index.freqstr
            self.v_table[v_table.index.freqstr] = self.v_table[
                v_table.index.freqstr
            ].join(v_table, how="outer")
        if c_table is not None:
            col = c_table.columns[0]
            if c_table.columns[0] in self.current:
                raise ValueError(f"{col} is already a named c_table")
            self.current[col] = "c_table"
            self.c_table[c_table.index.freqstr] = self.c_table[
                c_table.index.freqstr
            ].join(c_table, how="outer")
        if s_table is not None:
            col = s_table.columns[0]
            if s_table.columns[0] in self.current:
                raise ValueError(f"{col} is already a named s_table")
            self.current[col] = "s_table"
            self.s_table[s_table.index.freqstr] = self.s_table[
                s_table.index.freqstr
            ].join(s_table, how="outer")
        if e_table is not None:
            col = e_table.columns[0]
            if e_table.columns[0] in self.current:
                raise ValueError(f"{col} is already a named e_table")
            self.current[col] = "e_table"
            self.e_table[e_table.index.freqstr] = self.e_table[
                e_table.index.freqstr
            ].join(e_table, how="outer")
        if g_table is not None:
            col = g_table.columns[0]
            if g_table.columns[0] in self.current:
                raise ValueError(f"{col} is already a named g_table")
            self.current[col] = "g_table"
            self.g_table[g_table.index.freqstr] = self.g_table[
                g_table.index.freqstr
            ].join(g_table, how="outer")

    def _normalize_dates(self, date, time="00:00:00"):
        if date is None:
            return None
        return f"{date}T{time}"

    def _normalize_bools(self, torf):
        if torf is True or torf is False:
            return torf
        if isinstance(torf, str):
            if torf.lower() in ["y", "yes"]:
                return True
            if torf.lower() in ["n", "no"]:
                return False
        if torf:
            return True
        return False

    def digital_filter(
        self,
        series_name: str,
        new_series_name: str,
        filter_type: Literal["butterworth", "baseflow_separation"],
        filter_pass: Optional[Literal["low", "high", "band"]] = None,
        cutoff_frequency: Optional[float] = None,
        cutoff_frequency_1: Optional[float] = None,
        cutoff_frequency_2: Optional[float] = None,
        stages: Literal[1, 2, 3] = 1,
        alpha: Optional[float] = None,
        passes: Literal[1, 3] = 1,
        reverse_second_stage: Optional[Union[bool, Literal["yes", "no"]]] = None,
        clip_input: Union[bool, Literal["yes", "no"]] = False,
        clip_zero: Union[bool, Literal["yes", "no"]] = False,
    ):
        whichdf = self.current[series_name]
        series = self.series[whichdf][series_name]
        self.series[whichdf][new_series_name] = series
        self.current[new_series_name] = whichdf

    def erase_entity(
        self,
        series: Optional[str] = None,
        v_table: Optional[str] = None,
        c_table: Optional[str] = None,
        s_table: Optional[str] = None,
        e_table: Optional[str] = None,
        g_table: Optional[str] = None,
    ):
        if series is not None:
            self.series[self.current[series]] = self.series[self.current[series]].drop(
                series, axis="columns"
            )
        if v_table is not None:
            self.v_table[self.current[v_table]] = self.v_table[
                self.current[v_table]
            ].drop(v_table, axis="columns")
        if c_table is not None:
            self.c_table = self.c_table.drop(c_table, axis="columns")
        if s_table is not None:
            self.s_table = self.s_table.drop(s_table, axis="columns")
        if e_table is not None:
            self.e_table = self.e_table.drop(e_table, axis="columns")
        if g_table is not None:
            self.g_table = self.g_table.drop(g_table, axis="columns")

    def get_series_csv(
        self,
        file,
        new_series_name,
        use_cols=None,
        parse_dates=True,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        ts = pd.read_csv(file, use_cols=use_cols, parse_dates=parse_dates)
        if len(ts.columns) != 1:
            raise ValueError(
                tsutils.error_wrapper(
                    """
You can only bring in one column using "get_series_csv" instead you have
"{ts.columns}".
                                                   """
                )
            )
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
            names=pd.Index([new_series_name]),
        )
        self._join(series=ts)

    def get_series_hspfbin(
        self,
        file,
        new_series_name,
        interval,
        operationtype=None,
        id=None,
        variable_group=None,
        variable=None,
    ):
        from hspfbintoolbox.hspfbintoolobx import extract

        ts = extract(
            file, interval, f"{operationtype},{id},{variable_group},{variable}"
        )
        ts.columns = [new_series_name]
        self._join(series=ts)

    def get_series_wdm(
        self,
        file,
        new_series_name,
        dsn,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
        def_time="00:00:00",
        filter=None,
    ):
        ts = pd.DataFrame(
            get_series_wdm(file, {"dsn": dsn, "location": None, "constituent": None})
        )
        ts = tsutils.common_kwds(
            ts,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
            names=pd.Index([new_series_name]),
        )
        self._join(series=ts)

    def get_series_ssf(
        self,
        file,
        site: str,
        new_series_name: str,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        ts = pd.read_csv(
            file,
            header=None,
            parse_dates=[[1, 2]],
            index_col=[0],
            sep="\t",
            dtype={0: str},
        )
        try:
            ts = ts.pivot_table(
                index=ts.index,
                values=ts.columns.drop(ts.columns[0]),
                columns=ts.columns[0],
                aggfunc="first",
            )
        except ValueError:
            raise ValueError(
                tsutils.error_wrapper(
                    """
Duplicate index (time stamp and '{}') where found.
Found these duplicate indices:
{}
""".format(
                        ts.columns[0], ts.index.get_duplicates()
                    )
                )
            )
        ts.index.name = "Datetime"
        ts.columns = [i[1] for i in ts.columns]

        if site not in ts.columns:
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
The site name "{site}" is not in the available sites "{ts.columns}".
"""
                )
            )
        ts = tsutils.common_kwds(
            ts[site],
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
            names=pd.Index([new_series_name]),
        )
        self._join(series=ts)

    def hydrologic_indices(
        self,
        series_name: str,
        new_g_table_name: str,
        use_median: bool = False,
        stream_classification: Optional[
            Literal[
                "HARSH_INTERMITTENT",
                "FLASHY_INTERMITTENT",
                "SNOWMELT_PERENNIAL",
                "SNOW_RAIN_PERENNIAL",
                "GROUNDWATER_PERENNIAL",
                "FLASHY_PERENNIAL",
                "ALL_STREAMS",
            ]
        ] = None,
        flow_component: Optional[
            Literal[
                "AVERAGE_MAGNITUDE",
                "LOW_FLOW_MAGNITUDE",
                "HIGH_FLOW_MAGNITUDE",
                "LOW_FLOW_FREQUENCY",
                "HIGH_FLOW_FREQUENCY",
                "LOW_FLOW_DURATION",
                "HIGH_FLOW_DURATION",
                "TIMING",
                "RATE_OF_CHANGE",
            ]
        ] = None,
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
    ):
        from hydrotoolbox.hydrotoolbox import indices

    def reduce_time_span(
        self,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        whichdf = self.current[series_name]
        series = self.series[whichdf][series_name]
        series = tsutils.common_kwds(
            series[site],
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
            names=pd.Index([new_series_name]),
        )
        self._join(series=ts)

    def series_statistic(
        self,
        series_name,
        new_s_table_name,
        sum=False,
        mean=False,
        median=False,
        minmean=False,
        maxmean=False,
        std_dev=False,
        maximum=False,
        minimum=False,
        range=False,
        log=False,  # ?
        power=False,  # ?
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        whichdf = self.current[series_name]
        series = self.series[whichdf][series_name]
        rows = [
            "sum",
            "mean",
            "median",
            "minmean",
            "maxmean",
            "std_dev",
            "maximum",
            "minimum",
            "range",
        ]
        s_table = pd.DataFrame(
            [pd.NA] * len(rows), index=rows, names=[new_s_table_name]
        )
        if _normlize_bools(sum):
            s_table.loc["sum", :] = series.sum()
        if _normlize_bools(mean):
            s_table.loc["mean", :] = series.mean()
        if _normlize_bools(median):
            s_table.loc["median", :] = series.median()
        if _normlize_bools(minmean):
            s_table.loc["minmean", :] = series.minmean()  # ?
        if _normlize_bools(maxmean):
            s_table.loc["maxmean", :] = series.maxmean()  # ?
        if _normlize_bools(std_dev):
            s_table.loc["std_dev", :] = series.std_dev()  # ?
        if _normlize_bools(maximum):
            s_table.loc["maximum", :] = max(series)
        if _normlize_bools(minimum):
            s_table.loc["minimum", :] = min(series)
        if _normlize_bools(range):
            s_table.loc["range", :] = max(series) - min(series)
        self._join(s_table=s_table)

    def usgs_hysep(
        self,
        series_name,
        new_series_name,
        hysep_type,
        time_interval,
        date_1=None,
        time_1=None,
        date_2=None,
        time_2=None,
    ):
        from hydrotoolbox import hydrotoolbox

        whichdf = self.current[series_name]
        series = self.series[whichdf][series_name]
        if hysep_type.lower() == "fixed":
            new_series = hydrotoolbox.base_sep.fixed(series)
        elif hysep_type.lower() == "sliding":
            new_series = hydrotoolbox.base_sep.sliding(series)
        elif hysep_type.lower() == "interval":
            new_series = hydrotoolbox.base_sep.interval(series)
        else:
            raise ValueError(
                tsutils.error_wrapper(
                    """
The 'hysep_type' argument must be one of "fixed", "sliding", or "interval".  You gave {hysep_type}."""
                )
            )
        new_series = tsutils.common_kwds(
            new_series,
            start_date=self._normalize_dates(date_1, time_1),
            end_date=self._normalize_dates(date_2, time_2),
            names=pd.Index([new_series_name]),
        )
        self._join(series=new_series)


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
    data = []
    inblock = False
    data = []
    for index, line in enumerate(seq):
        nline = line.rstrip()

        # Handle comment lines and partial comment lines.  Everything from
        # a "#" to the end of the line is a comment.
        try:
            nline = nline[: nline.index("#")].rstrip()
        except ValueError:
            pass

        # Handle blank lines.
        if not nline:
            continue

        # Can't use 'tsutils.make_list(nline, sep=" ")' because need to have
        # number labels that are strings rather than integers.
        words = nline.split()
        keyword = words[0].lower()

        # Test for "START ..." at the beginning of the line, start collecting
        # lines and yield data when reaching the next "START ..."..
        if keyword == "start":
            inblock = True
            lindex = index + 1

        if inblock is True:
            data.append([keyword] + words[1:])

        if keyword == "end":
            inblock = False
            yield data, lindex
            data = []


@mando.command()
def run(infile, outfile=None, running_context=None):
    """Parse a tsproc file."""
    blocks = []
    lnumbers = []
    with open(infile, "r") as fpi:
        for group, lnumber in get_blocks(fpi):
            print(group, lnumber)
            # Unroll the block.

            # First find the maximum number of words from each line in the
            # group and store in "maxl".
            maxl = 2
            rollable = True
            for line in group:
                if line[0] in ["settings"]:
                    break
                if line[0] in [
                    "series_compare",
                    "series_equation",
                    "flow_duration",
                    "write_pest_files",
                    "hydrologic_indices",
                ]:
                    rollable = False
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
                for line in group[:-1]:
                    ngroup.append(line)
                    lnumbers.append(lnumber)

    if running_context is None:
        for block in blocks:
            if block[0] == ["start", "settings"]:
                for bl in block[1:]:
                    if bl[0] == "CONTEXT":
                        running_context = bl[1]
                        break
                break

    runblocks = []
    nnumbers = []
    for block, lnum in zip(blocks, lnumbers):
        for line in block:
            if (line[0] == "CONTEXT" and line[1] == running_context) or line[
                1
            ] == "all":
                runblocks.append(block)
                nnumbers.append(lnum)
                break
    print(runblocks)
    data = Tables()

    for block, lnum in zip(runblocks, nnumbers):
        keys = [i[0] for i in block]
        block_name = [i[1] for i in block if i[0] == "start"][0]
        args = [i.lower() for i in data.funcs[block_name]["args"]]
        kwds = {key.lower(): val for key, val in data.funcs[block_name]["kwds"].items()}
        if not all(elem in keys for elem in args):
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
All parameters in {args} are required for {block_name}.
You gave {keys}."""
                )
            )
        if all([item in args + list(kwds.keys()) for item in keys]):
            raise ValueError(
                tsutils.error_wrapper(
                    f"""
The available parameters for {block_name} are
{args + list(kwds.keys())}
but you gave
{set(args + list(kwds.keys()))-set(keys)}"""
                )
            )
        if block_name == "settings":
            continue
        block_context = [i[1] for i in block if i[0] == "context"]
        parameters = {i.lower(): j for i, j in block}
        kwds.update(parameters)
        parameters = {key.lower(): val for key, val in kwds.items() if val is not None}
        del parameters["start"]
        del parameters["context"]
        data.funcs[block_name]["f"](**parameters)
        print(data.series)


def main():
    """Set debug and run mando.main function."""
    if not os.path.exists("debug_tsblender"):
        sys.tracebacklimit = 0
    mando.main()


if __name__ == "__main__":
    main()
