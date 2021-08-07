# -*- coding: utf-8 -*-
""" Copyright (c) 2020 by RESPEC, INC.
Author: Robert Heaphy, Ph.D.

Based on MATLAB program by Seth Kenner, RESPEC
License: LGPL2
"""

import datetime

import numpy as np
import pandas as pd
from tstoolbox import tsutils

# look up attributes NAME, data type (Integer; Real; String) and data length by attribute number
attrinfo = {
    1: ("TSTYPE", "S", 4),
    2: ("STAID", "S", 16),
    11: ("DAREA", "R", 1),
    17: ("TCODE", "I", 1),
    27: ("TSBYR", "I", 1),
    28: ("TSBMO", "I", 1),
    29: ("TSBDY", "I", 1),
    30: ("TSBHR", "I", 1),
    32: ("TFILL", "R", 1),
    33: ("TSSTEP", "I", 1),
    34: ("TGROUP", "I", 1),
    45: ("STNAM", "S", 48),
    83: ("COMPFG", "I", 1),
    84: ("TSFORM", "I", 1),
    85: ("VBTIME", "I", 1),
    444: ("A444", "S", 12),
    443: ("A443", "S", 12),
    22: ("DCODE", "I", 1),
    10: ("DESCRP", "S", 80),
    7: ("ELEV", "R", 1),
    8: ("LATDEG", "R", 1),
    9: ("LNGDEG", "R", 1),
    288: ("SCENARIO", "S", 8),
    289: ("CONSTITUENT", "S", 8),
    290: ("LOCATION", "S", 8),
}

freq = {
    7: "100YS",
    6: "YS",
    5: "MS",
    4: "D",
    3: "H",
    2: "min",
    1: "S",
}  # pandas date_range() frequency by TCODE, TGROUP


def get_series_wdm(
    new_series_name,
    wdmfile,
    dsn,
    date_1=None,
    time_1=None,
    date_2=None,
    time_2=None,
    def_time="00:00:00",
    filter=None,
):
    """get_series_wdm"""
    iarray = np.fromfile(wdmfile, dtype=np.int32)
    farray = np.fromfile(wdmfile, dtype=np.float32)

    if iarray[0] != -998:
        raise ValueError(
            tsutils.error_wrapper(
                f"""
{wdmfile} is not a WDM file, magic number is not -990."""
            )
        )
    nrecords = iarray[28]  # first record is File Definition Record
    ntimeseries = iarray[31]

    dsns = {}
    for index in range(512, nrecords * 512, 512):
        if (
            not (
                iarray[index] == 0
                and iarray[index + 1] == 0
                and iarray[index + 2] == 0
                and iarray[index + 3]
            )
            and iarray[index + 5] == 1
        ):
            dsns[iarray[index + 4]] = index

    if len(dsns) != ntimeseries:
        raise ValueError(
            tsutils.error_wrapper(
                """
Wrong number of DSN records found in WDM file {wdmfile}"""
            )
        )
    if dsn not in dsns:
        raise ValueError(
            tsutils.error_wrapper(
                """
DSN {dsn} not in WDM file {wdmfile}"""
            )
        )

    farray = np.fromfile(wdmfile, dtype=np.float32)

    summary = []
    summaryindx = []

    index = dsns[dsn]

    # get layout information for TimeSeries Dataset frame
    psa = iarray[index + 9]
    if psa > 0:
        sacnt = iarray[index + psa - 1]
    pdat = iarray[index + 10]
    pdatv = iarray[index + 11]
    frepos = iarray[index + pdat]

    # get attributes
    dattr = {
        "TSBDY": 1,
        "TSBHR": 1,
        "TSBMO": 1,
        "TSBYR": 1900,
        "TFILL": -999.0,
    }  # preset defaults
    for i in range(psa + 1, psa + 1 + 2 * sacnt, 2):
        id = iarray[index + i]
        ptr = iarray[index + i + 1] - 1 + index
        if id not in attrinfo:
            continue

        name, atype, length = attrinfo[id]
        if atype == "I":
            dattr[name] = iarray[ptr]
        elif atype == "R":
            dattr[name] = farray[ptr]
        else:
            dattr[name] = "".join(
                [itostr(iarray[k]) for k in range(ptr, ptr + length // 4)]
            ).strip()

    # Get timeseries timebase data
    records = []
    for i in range(pdat + 1, pdatv - 1):
        a = iarray[index + i]
        if a != 0:
            records.append(splitposition(a))

    srec, soffset = records[0]
    start = splitdate(iarray[srec * 512 + soffset])

    sprec, spoffset = splitposition(frepos)
    finalindex = sprec * 512 + spoffset

    # calculate number of data points in each group, tindex is final index for storage
    tgroup = dattr["TGROUP"]
    tstep = dattr["TSSTEP"]
    tcode = dattr["TCODE"]
    cindex = pd.date_range(start=start, periods=len(records) + 1, freq=freq[tgroup])
    tindex = pd.date_range(start=start, end=cindex[-1], freq=str(tstep) + freq[tcode])
    counts = np.diff(np.searchsorted(tindex, cindex))

    ## Get timeseries data
    floats = np.zeros(sum(counts), dtype=np.float32)
    findex = 0
    for (rec, offset), count in zip(records, counts):
        findex = getfloats(
            iarray, farray, floats, findex, rec, offset, count, finalindex
        )

    series = pd.Series(floats[:findex], index=tindex[:findex])
    series.name = new_series_name
    return series


def todatetime(yr=1900, mo=1, dy=1, hr=0):
    """takes yr,mo,dy,hr information then returns its datetime64"""
    if hr == 24:
        return datetime.datetime(yr, mo, dy, 23) + pd.Timedelta(1, "h")
    else:
        return datetime.datetime(yr, mo, dy, hr)


def splitdate(x):
    """splits WDM int32 DATWRD into year, month, day, hour -> then returns its datetime64"""
    return todatetime(
        x >> 14, x >> 10 & 0xF, x >> 5 & 0x1F, x & 0x1F
    )  # args: year, month, day, hour


def splitposition(x):
    """splits int32 into (record, offset), converting to Pyton zero based indexing"""
    return ((x >> 9) - 1, (x & 0x1FF) - 1)


def itostr(i):
    return (
        chr(i & 0xFF) + chr(i >> 8 & 0xFF) + chr(i >> 16 & 0xFF) + chr(i >> 24 & 0xFF)
    )


def getfloats(iarray, farray, floats, findex, rec, offset, count, finalindex):
    index = rec * 512 + offset + 1
    stop = (rec + 1) * 512
    cntr = 0
    while cntr < count and findex < len(floats):
        if index >= stop:
            rec = (
                iarray[rec * 512 + 3] - 1
            )  # 3 is forward data pointer, -1 is python indexing
            index = rec * 512 + 4  # 4 is index of start of new data
            stop = (rec + 1) * 512

        x = iarray[index]  # control word, don't need most of it here
        comp = x >> 5 & 0x3
        nval = x >> 16

        index += 1
        if comp == 0:
            for k in range(nval):
                if findex >= len(floats):
                    return findex
                floats[findex] = farray[index + k]
                findex += 1
            index += nval
        else:
            for k in range(nval):
                if findex >= len(floats):
                    return findex
                floats[findex] = farray[index]
                findex += 1
            index += 1
        cntr += nval
    return findex
