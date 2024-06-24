try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional, Union

from tsblender.toolbox_utils.src.toolbox_utils import tsutils


@validate_call
def get_series_xlsx(
    self,
    new_series_name: str,
    file: str,
    sheet: Union[int, str] = 1,
    column: Union[int, str] = 1,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from an Excel file."""
    ts = tsutils.common_kwds(
        f"{file},{sheet}",
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    try:
        ts = ts.iloc[:, int(column) - 1]
    except ValueError:
        ts = ts.loc[:, column]
    self._join(new_series_name, series=ts)
