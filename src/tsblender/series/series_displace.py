try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call


@validate_call
def series_displace(
    self,
    series_name,
    new_series_name,
    lag_increment: int,
    fill_value,
):
    """Displace a time series by a given lag."""
    series = self._get_series(series_name)
    series = series.shift(lag_increment)
    series[-lag_increment:] = fill_value  # Verify what TSPROC does.
    self._join(new_series_name, series=series)
