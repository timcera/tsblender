try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call


@validate_call
def series_difference(
    self,
    series_name: str,
    new_series_name: str,
):
    """Calculate the difference between two time series."""
    series = self._get_series(series_name)
    series = series.diff()
    self._join(new_series_name, series=series)
