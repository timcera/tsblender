from typing import Optional

import pandas as pd
from pydantic import Field

try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from toolbox_utils import tsutils
from toolbox_utils.readers.wdm import wdm_extract as _get_series_wdm
from typing_extensions import Annotated


@validate_call
def get_series_wdm(
    self,
    file: str,
    new_series_name: str,
    dsn: Annotated[int, Field(gt=0, le=32000)] = 1,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
    def_time="00:00:00",
    filter: Optional[int] = None,
):
    """Get a time series from a WDM file."""
    ts = pd.DataFrame(_get_series_wdm(file, dsn))
    hh, mm, ss = self._normalize_times(def_time)
    if hh == 24:
        delta = pd.Timedelta(days=1, minutes=mm, seconds=ss)
    else:
        delta = pd.Timedelta(hours=hh, minutes=mm, seconds=ss)
    ts.index = ts.index + delta
    ts = tsutils.common_kwds(
        ts,
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    self._join(new_series_name, series=ts)
