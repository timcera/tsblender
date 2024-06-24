try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional

import pandas as pd

from tsblender.toolbox_utils.src.toolbox_utils import tsutils


@validate_call
def get_series_ufore_hydro(
    self,
    file: str,
    new_series_name: str,
    model_reference_date: str,
    model_reference_time: str,
    time_increment: str,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a UFORE hydrology file."""
    model_reference_date = self._normalize_datetimes(
        model_reference_date, model_reference_time
    )
    time_increment = pd.Timedelta(time_increment)

    with open(file, encoding="ascii") as f:
        nterm = int(f.readline())
        ts = [float(f.readline()) for _ in range(nterm)]
    ts_range = pd.date_range(
        start=model_reference_date,
        periods=nterm,
        freq=time_increment,
    )
    ts = pd.DataFrame(ts, index=ts_range)

    ts = tsutils.common_kwds(
        ts,
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    self._join(new_series_name, series=ts)
