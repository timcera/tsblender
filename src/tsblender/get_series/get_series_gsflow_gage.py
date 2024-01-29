import pandas as pd

try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

from pydantic import Field
from toolbox_utils import tsutils
from typing_extensions import Annotated


@validate_call
def get_series_gsflow_gage(
    self,
    file: str,
    data_type: str,
    new_series_name: str,
    model_reference_date: str,
    model_reference_time: str,
    time_units_per_day: Annotated[int, Field(gt=0)] = 1,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a GSFLOW gage file."""
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
        + pd.to_datetime(f"{model_reference_date} {model_reference_time}")
    ) - unit

    ts = tsutils.common_kwds(
        ts,
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
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
