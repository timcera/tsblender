try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from contextlib import suppress
from typing import Optional, Union

import pandas as pd
from toolbox_utils import tsutils
from toolbox_utils.readers.plotgen import plotgen_extract as _get_series_pgen


@validate_call
def get_series_plotgen(
    self,
    file: str,
    label,
    new_series_name: str,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a HSPF PLOTGEN file."""
    if isinstance(label, str):
        label = [label]
    if isinstance(new_series_name, str):
        new_series_name = [new_series_name]
    ts = _get_series_pgen(file)
    ts = tsutils.common_kwds(
        ts,
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    for lb, nsn in zip(label, new_series_name):
        try:
            nts = ts[lb]
        except KeyError as exc:
            raise KeyError(
                tsutils.error_wrapper(
                    f"""
                    The time-series "{lb}" is not available in file
                    "{file}". The available time-series are {ts.columns}.
                    """
                )
            ) from exc
        self.series_dates[nsn.upper()] = [nts.index[0], nts.index[-1]]
        self._join(nsn.upper(), series=nts)
