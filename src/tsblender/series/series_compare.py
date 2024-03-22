try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

import numpy as np
import pandas as pd
from tstoolbox.functions.gof import gof


@validate_call
def index_of_agreement(self, sim, obs, obs_par_b=None, exponent=1):
    """Calculate the index of agreement."""
    if obs_par_b is None:
        obs_par_b = obs.mean()
    return 1 - (
        np.sum(abs(obs - sim)) ** exponent
        / np.sum(np.abs(sim - obs_par_b) + np.abs(obs - obs_par_b)) ** exponent
    )


@validate_call
def coefficient_of_efficiency(self, sim, obs, obs_par_b=None, exponent=1):
    """Calculate the coefficient of efficiency."""
    if obs_par_b is None:
        obs_par_b = obs.mean()
    return 1 - (
        np.sum(abs(obs - sim)) ** exponent / np.sum(abs(obs - obs_par_b)) ** exponent
    )


@validate_call
def series_compare(
    self,
    series_name_sim: str,
    series_name_obs: str,
    new_c_table_name: str,
    bias: bool = False,
    standard_error: bool = False,
    relative_bias: bool = False,
    relative_standard_error: bool = False,
    nash_sutcliffe: bool = False,
    coefficient_of_efficiency: bool = False,
    index_of_agreement: bool = False,
    volumetric_efficiency: bool = False,
    exponent: int = 2,
    series_name_base: str = "",
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Calculate comparison statistics for two time series."""
    if exponent not in [1, 2]:
        raise ValueError(f"exponent must be 1 or 2, not {exponent}")

    series_sim = self._prepare_series(
        series_name_sim,
        date_1=date_1,
        time_1=time_1,
        date_2=date_2,
        time_2=time_2,
    )
    series_obs = self._prepare_series(
        series_name_obs,
        date_1=date_1,
        time_1=time_1,
        date_2=date_2,
        time_2=time_2,
    )
    if series_name_base:
        series_base = self._prepare_series(
            series_name_base,
            date_1=date_1,
            time_1=time_1,
            date_2=date_2,
            time_2=time_2,
        )
    else:
        series_base = None

    stats = {}
    if self._normalize_bools(bias):
        stats["Bias:"] = gof(stats="me", sim_col=series_sim, obs_col=series_obs)[0][1]
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
        stats["Coefficient of efficiency:"] = self.coefficient_of_efficiency(
            series_sim, series_obs, obs_par_b=series_base, exponent=exponent
        )
    if self._normalize_bools(index_of_agreement):
        stats["Index of agreement:"] = self.index_of_agreement(
            series_sim, series_obs, obs_par_b=series_base, exponent=exponent
        )
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
        "num_terms": min(len(series_sim), len(series_obs)),
    }
