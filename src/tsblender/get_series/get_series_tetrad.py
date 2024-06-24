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
    """Get a time series from a TETRAD file.

    I can't find the TETRAD file format documentation, so I'm not sure what
    the arguments are supposed to be.  I don't think this is properly
    implemented in TSPROC either.  Placeholder for now.
    """
