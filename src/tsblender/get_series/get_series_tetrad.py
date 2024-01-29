try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Optional


@validate_call
def get_series_tetrad(
    self,
    file: str,
    new_series_name: str,
    well_name: str,
    object_name: str,
    model_reference_date: str,
    model_reference_time: str,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a TETRAD file."""
    start_date = self._normalize_datetimes(date_1, time_1)
    end_date = self._normalize_datetimes(date_2, time_2)
    model_reference_date = self._normalize_datetimes(
        model_reference_date, model_reference_time
    )
