try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from contextlib import suppress
from typing import Optional, Union

from tsblender.toolbox_utils.src.toolbox_utils import tsutils


@validate_call
def get_series_csv(
    self,
    file: str,
    new_series_name: str,
    usecol: Optional[Union[int, str]] = None,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a CSV file."""
    with suppress(ValueError):
        usecol = int(usecol)
    ts = tsutils.common_kwds(
        f"{file},{usecol}",
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    self._join(new_series_name, series=ts)
