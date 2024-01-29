try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

import pandas as pd
from toolbox_utils import tsutils


@validate_call
def get_series_statvar(
    self,
    file: str,
    variable_name,
    location_id,
    new_series_name,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a STATVAR file."""
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
        for _ in range(num_series):
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
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
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
