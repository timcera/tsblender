from typing import Literal, Optional, Union

import pandas as pd

try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call


@validate_call
def series_clean(
    self,
    series_name: str,
    new_series_name: str,
    substitute_value: Union[float, Literal["delete"]],
    lower_erase_boundary: Optional[float] = None,
    upper_erase_boundary: Optional[float] = None,
):
    """Clean a time series by replacing/removing values outside of a range."""
    if substitute_value == "delete":
        substitute_value = pd.NA
    series = self._get_series(series_name)
    if isinstance(lower_erase_boundary, (int, float)):
        series[series >= lower_erase_boundary] = substitute_value
    if isinstance(upper_erase_boundary, (int, float)):
        series[series <= upper_erase_boundary] = substitute_value
    self._join(new_series_name, series=series)
