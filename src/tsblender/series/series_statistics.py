try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

import numpy as np
import pandas as pd


@validate_call
def series_statistics(
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
    """Calculate statistics for a time series."""
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
        key: self._normalize_bools(value) for key, value in minmaxmeans.items() if value
    }
    rows.extend(iter(minmaxmeans))
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
            grouped = series.groupby(np.arange(len(series.index)) // interval).mean()
            if "max" in stat:
                s_table.loc[key, :] = grouped.max()
            if "min" in stat:
                s_table.loc[key, :] = grouped.min()
    self._join(new_s_table_name, s_table=s_table)
    self.s_table_metadata[new_s_table_name.upper()] = {
        "source_name": series_name,
        "start_date": series.index[0],
        "end_date": series.index[-1],
        "log_transformed": log,
        "exponent": power,
    }
