try:
    from pydantic import validate_call
except ImportError:
    from pydantic import validate_arguments as validate_call

from typing import Literal, Optional

from pydantic import Field
from typing_extensions import Annotated

from tsblender.toolbox_utils.src.toolbox_utils import tsutils
from tsblender.toolbox_utils.src.toolbox_utils.readers.hbn import (
    hbn_extract as _get_series_hbn,
)


@validate_call
def get_series_hspfbin(
    self,
    file: str,
    new_series_name: str,
    interval: Literal["yearly", "monthly", "daily", "bivl"],
    operationtype: Literal["PERLND", "IMPLND", "RCHRES", "BMPRAC"],
    id: Annotated[int, Field(gt=0, lt=1000)],
    variable: str,
    date_1: Optional[str] = None,
    time_1: Optional[str] = None,
    date_2: Optional[str] = None,
    time_2: Optional[str] = None,
):
    """Get a time series from a HSPF binary file."""
    ts = _get_series_hbn(
        file,
        interval,
        f"{operationtype},{id},,{variable}",
    )
    if len(ts.columns) == 0:
        raise ValueError(
            tsutils.error_wrapper(
                f"""
                No data found in the file when processing the block
                {self.block_name} at line number {self.line_number}.
                """
            )
        )
    ts = tsutils.common_kwds(
        ts,
        start_date=self._normalize_datetimes(date_1, time_1),
        end_date=self._normalize_datetimes(date_2, time_2),
    )
    self._join(new_series_name, series=ts)
