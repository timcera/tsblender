try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

import pandas as pd
from toolbox_utils import tsutils


@validate_call
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
    """Get a time series from a SSF file."""
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
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
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
