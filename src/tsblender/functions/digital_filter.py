# -*- coding: utf-8 -*-
import mandor


def digital_filter_cli(series_name):
    pass


filter_types = ["butterworth", "baseflow_separation"]
filter_passes = ["low", "band", "high"]


@mandor.command(
    series_name=[
        (lambda x: len(x) > 2, "must be longer than 2 charachters"),
        (lambda x: x[0] not in range(10), "first charachter can't be a number"),
        (
            lambda x: all(i in mandor.alphanum for i in x),
            "can't contain special characters",
        ),
    ],
    filter_type=[
        (lambda x: x in filter_types, "must be one of {}".format(filter_types))
    ],
    filter_pass=[
        (lambda x: x in filter_passes, "must be one of {}".format(filter_passes))
    ],
    cutoff_frequency=[
        (lambda x: isinstance(float(x), float), "must be float"),
        (lambda x: x > 0, "must be greater than 0"),
    ],
)
def digital_filter(series_name):
    """Butterworth and baseflow filters.

    Parameters
    ----------
    series_name
        Mandatory. The name of the time series on which
        filtering operations will be carried out.
    filter_type
        Mandatory. The type of filter being implemented. "butterworth" or
        “baseflow_separation” .
    filter_pass
        Mandatory if FILTER_TYPE is “butterworth”; disallowed otherwise.
        Informs TSPROC whether to carry out low, band, or high pass filtering.
        One of “low,” “band,” or “high”.
    cutoff_frequency
        Mandatory if FILTER_TYPE is “butterworth” and
        FILTER_PASS is “high” or “low”; disallowed
        otherwise. For a high pass filter, the 3dB point of
        low frequency roll-off. For a low pass, filter the
        3dB point of high frequency roll-off. Frequency
        in days: 1. float
    cutoff_frequency_1
        Mandatory if FILTER_TYPE is “butterworth” and
        FILTER_PASS is “band”; disallowed otherwise.
        The 3dB point of low frequency roll-off. Frequency in days 1. float
    cutoff_frequency_2
        Mandatory if FILTER_TYPE is “butterworth” and
        FILTER_PASS is “band”; disallowed otherwise.
        The 3dB point of high frequency roll-off. Frequency in days 1. float
    stages
        Optional if FILTER_TYPE is “butterworth”; disallowed otherwise.
        Number of filter stages. The more stages, the steeper is the high
        and/or low frequency roll-off. Default is 1. int 1, 2, or 3
    alpha
        Mandatory if FILTER_TYPE is “baseflow_separation”; disallowed
        otherwise. The assumed relative decay rate of baseflow.
        float > 0 normally in the range 0.9 to 0.975
    passes
        Optional if FILTER_TYPE is “baseflow_separation”; disallowed
        otherwise. The number of filter passes. Default is 1.
        Integer. 1 or 3 only.
    reverse_second_stage
        Optional. If FILTER_TYPE is set to “butterworth,”
        STAGES is set to 2, and FILTER_PASS is set to
        “low,” then the second filter pass is performed
        in the reverse direction, thereby nullifying any
        phase shift incurred in the first low pass filter
        pass. bool, default is False
    clip_input
        Optional for baseflow separation filter type; disallowed for
        butterworth. If activated, prevents terms of filtered time series
        from exceeding terms of original time series. bool, Default is False
    clip_zero
        Optional for baseflow separation filter type; disallowed
        for butterworth. If activated, prevents terms of filtered
        time series from becoming negative. bool, Default is False.
    """
