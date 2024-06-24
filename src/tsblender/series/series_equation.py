import re

try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call


def _series_equation(
    self,
    equation,
):
    """Create a new time series from an equation."""
    if isinstance(equation, list):
        equation = "".join(equation)
    equation = equation.strip().lower()

    # series
    keys = sorted(self.current_series.keys(), key=len, reverse=True)
    series_in_equation = ""
    for i in keys:
        equation = equation.replace(i.lower(), f"self._get_series('{i.upper()}')")
        if i.upper() in equation:
            series_in_equation = i.upper()

    # v_table
    for i in self.v_table.columns:
        equation = equation.replace(i.lower(), f"self._get_v_table('{i.upper()}')")

    # c_table
    for i in self.c_table.columns:
        equation = equation.replace(i.lower(), f"self._get_c_table('{i.upper()}')")

    # s_table
    for i in self.s_table.columns:
        equation = equation.replace(i.lower(), f"self._get_s_table('{i.upper()}')")

    # e_table
    for i in self.e_table.columns:
        equation = equation.replace(i.lower(), f"self._get_e_table('{i.upper()}')")

    # g_table
    for i in self.g_table.columns:
        equation = equation.replace(i.lower(), f"self._get_g_table('{i.upper()}')")

    equation = equation.replace("^", "**")
    if series_in_equation and "@_days_" in equation:
        equation = equation.replace(
            "@_days_start_year",
            f"self._get_series('{series_in_equation.upper()}').index.dayofyear",
        )
        equation = re.sub(
            "@_days_.(\\d{2}/\\d{2}/\\d{4}_\\d{2}:\\d{2}:\\{2}).",
            f"(self._get_series('{series_in_equation.upper()}').index.to_julian_date()-pd.Timestamp(r'\1').index.to_julian_date())",
            equation,
        )
    return eval(equation)


@validate_call
def series_equation(
    self,
    new_series_name: str,
    equation,
):
    series = self._series_equation(equation)
    self._join(new_series_name, series=series)
