try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

import pandas as pd


@validate_call
def series_base_level(
    self,
    series_name: str,
    substitute,
    new_series_name: str,
    base_level_series_name: str,
    base_level_date: str,
    base_level_time: str,
    negate: bool = False,
):
    """Create a new time series with a base level."""
    series = self._get_series(series_name)
    base_level = self._get_series(base_level_series_name)
    base = base_level[pd.to_datetime(f"{base_level_date} {base_level_time}")]
    series = series - base
    if self._normalize_bools(negate):
        series = -series
    if self._normalize_bools(substitute):
        new_series_name = series_name
        self.erase_entity(series_name=new_series_name)
    self._join(new_series_name, series=series)
